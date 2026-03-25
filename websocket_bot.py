import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re

# ----------------------------------------------------
# 1. 기본 설정 및 인증
# ----------------------------------------------------
st.set_page_config(page_title="수급 스나이퍼 PRO", layout="wide")
st.title("⚡ 1:1 수급 비교 & 주포 자동 스나이퍼")
st.caption("두 창구를 비교함과 동시에, 뒤에서는 10대 메이저 창구를 스캔하여 진짜 주포를 찾아냅니다.")

app_key = st.secrets["APP_KEY"]
app_secret = st.secrets["APP_SECRET"]
host_url = "https://mockapi.kiwoom.com"  # 클라우드는 무조건 모의투자(mockapi) 주소 사용

@st.cache_data(ttl=3600)
def get_access_token():
    url = f"{host_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8"}
    data = {"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}
    return requests.post(url, headers=headers, json=data).json()

@st.cache_data(ttl=86400) 
def get_broker_list(token):
    url = f"{host_url}/api/dostk/stkinfo"
    headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10102", "authorization": f"Bearer {token}"}
    res = requests.post(url, headers=headers, json={})
    data = res.json()
    broker_dict = {}
    if "list" in data:
        for item in data["list"]: 
            broker_dict[f"{item['name']}({item['code']})"] = item["code"]
    return broker_dict

def get_historical_broker_data(token, stock_code, brk_code, max_pages=100):
    url = f"{host_url}/api/dostk/stkinfo"
    all_data = []
    next_key = ""
    for i in range(max_pages): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10052", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        req_data = {"mmcm_cd": brk_code, "stk_cd": stock_code, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}
        
        response = requests.post(url, headers=headers, json=req_data)
        if response.status_code != 200:
            time.sleep(3)
            continue
            
        res_json = response.json()
        chunk = res_json.get('trde_ori_mont_trde_qty', [])
        if not chunk: break
        
        all_data.extend(chunk)
        
        last_time = chunk[-1].get('tm', chunk[-1].get('stck_cntg_hour', ''))
        if last_time and last_time <= "090000": break
            
        cont_yn = response.headers.get('cont-yn', response.headers.get('tr-cont', 'N'))
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if str(cont_yn).upper() not in ['Y', 'M'] or not next_key: break
        time.sleep(0.3) 
    return all_data

# ----------------------------------------------------
# 2. 메인 화면 및 실행부
# ----------------------------------------------------
token_response = get_access_token()
auth_token = token_response.get('token')

if not auth_token:
    st.error("🚨 API 토큰 발급 실패! 인증 정보를 확인하세요.")
    st.stop()

st.sidebar.header("📅 스캔 설정")
stock_number = st.sidebar.text_input("종목코드 (예: 005930)", value="005930")
selected_date = st.sidebar.date_input("날짜 선택", datetime.now())
target_date_str = selected_date.strftime('%Y%m%d')

broker_dict = get_broker_list(auth_token)
broker_names = sorted(list(broker_dict.keys())) if broker_dict else ["데이터없음"]

default_idx1 = next((i for i, name in enumerate(broker_names) if name.startswith("키움증권(")), 0)
selected_broker_name1 = st.sidebar.selectbox("🔎 기준 창구 (주체)", broker_names, index=default_idx1)
target_broker_code1 = broker_dict.get(selected_broker_name1, "")

default_idx2 = next((i for i, name in enumerate(broker_names) if name.startswith("신한투자증권(")), 0)
selected_broker_name2 = st.sidebar.selectbox("🔎 비교 창구 (1:1)", broker_names, index=default_idx2)
target_broker_code2 = broker_dict.get(selected_broker_name2, "")

lag_seconds = st.sidebar.slider("⏱️ 창구 시간 보정 (초)", 0, 180, 130)

# ⭐️ 자동 검색할 10대 엘리트 창구 풀 생성
elite_keywords = ["신한", "모건", "제이피", "골드만", "메릴린치", "삼성", "한국", "미래", "NH", "KB"]
elite_brokers = {name: code for name, code in broker_dict.items() if any(k in name for k in elite_keywords)}

if st.sidebar.button("🚀 1:1 분석 & 주포 자동 스캔"):
    with st.spinner("초고속 데이터 수집 및 주포 분석 중입니다..."):
        
        def process_broker_data(raw_data, lag_sec, suffix):
            if not raw_data:
                return pd.DataFrame(columns=['Datetime', f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}']).set_index('Datetime')
                
            df_b = pd.DataFrame(raw_data)
            time_col_b = 'tm' if 'tm' in df_b.columns else 'stck_cntg_hour'
            
            df_b['Datetime_Raw'] = pd.to_datetime(target_date_str + df_b[time_col_b], format='%Y%m%d%H%M%S', errors='coerce')
            df_b['Datetime'] = df_b['Datetime_Raw'] - pd.Timedelta(seconds=lag_sec) 
            df_b['Datetime'] = df_b['Datetime'].dt.floor('min')
            
            def parse_volume(row):
                tp_str = str(row.get('tp', ''))
                qty_str = str(row.get('mont_trde_qty', '0')).replace(',', '')
                if '-' in qty_str or '매도' in tp_str:
                    return 0, int(qty_str.replace('-', '').replace('+', '')) if qty_str else 0
                else:
                    return int(qty_str.replace('+', '').replace('-', '')) if qty_str else 0, 0

            df_b[['Buy_Vol', 'Sell_Vol']] = df_b.apply(parse_volume, axis=1, result_type='expand')
            
            if 'acc_netprps' in df_b.columns:
                df_b['Net_Raw'] = pd.to_numeric(df_b['acc_netprps'].astype(str).str.replace('+', '', regex=False).str.replace(',', '', regex=False), errors='coerce').fillna(0).astype(int)
            else:
                df_b['Net_Raw'] = 0
                
            df_b_min = df_b.groupby('Datetime').agg({'Buy_Vol': 'sum', 'Sell_Vol': 'sum', 'Net_Raw': 'last'})
            df_b_min.rename(columns={'Buy_Vol': f'Buy_1m_{suffix}', 'Sell_Vol': f'Sell_1m_{suffix}', 'Net_Raw': f'Cum_Net_{suffix}'}, inplace=True)
            return df_b_min

        # --- [1단계] 사용자가 선택한 1:1 창구 분석 ---
        brk_raw1 = get_historical_broker_data(auth_token, stock_number, target_broker_code1)
        brk_raw2 = get_historical_broker_data(auth_token, stock_number, target_broker_code2)
        
        df1 = process_broker_data(brk_raw1, lag_seconds, 'brk1')
        df2 = process_broker_data(brk_raw2, lag_seconds, 'brk2')
        
        df = df1.join(df2, how='outer').fillna(0)
        df['Net_1m_brk1'] = df['Buy_1m_brk1'] - df['Sell_1m_brk1']
        df['Net_1m_brk2'] = df['Buy_1m_brk2'] - df['Sell_1m_brk2']
        df['Cum_Net_brk1'] = df['Net_1m_brk1'].cumsum()
        df['Cum_Net_brk2'] = df['Net_1m_brk2'].cumsum()

        df_corr_active = df[(df['Net_1m_brk1'] != 0) | (df['Net_1m_brk2'] != 0)]
        real_corr = df_corr_active['Net_1m_brk1'].corr(df_corr_active['Net_1m_brk2']) if len(df_corr_active) >= 3 else 0.0
        if pd.isna(real_corr): real_corr = 0.0

        # UI 출력
        st.subheader("📊 사용자가 선택한 1:1 창구 비교")
        c_color = "#0066ff" if real_corr <= -0.5 else "#ff4d4d" if real_corr >= 0.5 else "gray"
        st.markdown(f"<h3 style='color: {c_color}; text-align: center;'>상관계수: {real_corr:+.2f}</h3>", unsafe_allow_html=True)
        
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08, row_heights=[0.6, 0.4], subplot_titles=("📈 누적 순매수 흐름", "⚡ 1분봉 순매수 공방전"))
        fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk1'], mode='lines', name=f"{selected_broker_name1}", line=dict(color='#ff4d4d', width=3)), row=1, col=1)
        fig.add_trace(go.Scatter(x=df.index, y=df['Cum_Net_brk2'], mode='lines', name=f"{selected_broker_name2}", line=dict(color='#0066ff', width=3)), row=1, col=1)
        fig.add_trace(go.Bar(x=df.index, y=df['Net_1m_brk1'], name=f"{selected_broker_name1}", marker_color='#ff4d4d', opacity=0.7), row=2, col=1)
        fig.add_trace(go.Bar(x=df.index, y=df['Net_1m_brk2'], name=f"{selected_broker_name2}", marker_color='#0066ff', opacity=0.7), row=2, col=1)
        fig.update_layout(height=600, template='plotly_white', barmode='group', showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # --- [2단계] 자동 주포 스나이퍼 (10대 창구 스캔) ---
        st.subheader(f"🎯 [{selected_broker_name1}] 기준 최저 상관 창구 자동 포착")
        
        progress_bar = st.progress(0, text="엘리트 메이저 창구 스캔 중...")
        best_corr = 1.0
        best_broker_name = None
        best_df = None

        elite_items = list(elite_brokers.items())
        for idx, (c_name, c_code) in enumerate(elite_items):
            progress_bar.progress((idx + 1) / len(elite_items), text=f"스캔 중: {c_name}")
            if c_code == target_broker_code1: continue
                
            c_raw = get_historical_broker_data(auth_token, stock_number, c_code, max_pages=50) # 속도를 위해 50페이지 제한
            df_c = process_broker_data(c_raw, lag_seconds, 'cand')
            
            df_merged = df1.join(df_c, how='outer').fillna(0)
            df_merged['Net_1m_brk1'] = df_merged['Buy_1m_brk1'] - df_merged['Sell_1m_brk1']
            df_merged['Net_1m_cand'] = df_merged['Buy_1m_cand'] - df_merged['Sell_1m_cand']
            df_merged['Cum_Net_cand'] = df_merged['Net_1m_cand'].cumsum()
            
            # 동시 0구간 제외 로직 적용
            df_active = df_merged[(df_merged['Net_1m_brk1'] != 0) | (df_merged['Net_1m_cand'] != 0)]
            
            if len(df_active) >= 3:
                corr = df_active['Net_1m_brk1'].corr(df_active['Net_1m_cand'])
                if pd.isna(corr): corr = 0.0
                if corr < best_corr:
                    best_corr = corr
                    best_broker_name = c_name
                    best_df = df_merged

        progress_bar.empty()

        # ----------------------------------------------------
        # 3. 1등 주포 발표 및 시각화
        # ----------------------------------------------------
        if best_broker_name and best_corr <= -0.3:
            st.success(f"🏆 주포 포착 완료! 오늘 [{selected_broker_name1}] 물량을 가장 악랄하게 털어먹은 창구는 **{best_broker_name}** 입니다.")
            
            st.markdown(
                f"""
                <div style='padding: 20px; border-radius: 10px; border: 2px solid #0066ff; background-color: #f8f9fa; text-align: center;'>
                    <h3 style='margin:0; color: #333;'>[{selected_broker_name1}] <span style='color:gray;'>vs</span> <span style='color:#0066ff;'>[{best_broker_name}]</span></h3>
                    <h1 style='margin: 10px 0; font-size: 50px; color: #0066ff;'>{best_corr:+.2f}</h1>
                    <p style='margin:0; font-size: 18px; color: #0066ff; font-weight: bold;'>🔥 완벽한 역상관 (진짜 세력)</p>
                </div>
                <br>
                """, unsafe_allow_html=True
            )

            # 최적 창구의 1분봉 차트 출력
            fig_best = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08, row_heights=[0.6, 0.4])
            fig_best.add_trace(go.Scatter(x=best_df.index, y=best_df['Cum_Net_brk1'], mode='lines', name=selected_broker_name1, line=dict(color='#ff4d4d', width=3)), row=1, col=1)
            fig_best.add_trace(go.Scatter(x=best_df.index, y=best_df['Cum_Net_cand'], mode='lines', name=best_broker_name, line=dict(color='#0066ff', width=3)), row=1, col=1)
            fig_best.add_trace(go.Bar(x=best_df.index, y=best_df['Net_1m_brk1'], name=selected_broker_name1, marker_color='#ff4d4d', opacity=0.7), row=2, col=1)
            fig_best.add_trace(go.Bar(x=best_df.index, y=best_df['Net_1m_cand'], name=best_broker_name, marker_color='#0066ff', opacity=0.7), row=2, col=1)
            fig_best.update_layout(height=600, template='plotly_white', barmode='group', showlegend=False)
            st.plotly_chart(fig_best, use_container_width=True)
            
        else:
            st.info("오늘 이 종목에서는 뚜렷한 역상관을 보이는 메이저 세력 창구가 발견되지 않았습니다. (세력 이탈 또는 소형 창구 주도)")
