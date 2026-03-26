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

def get_historical_broker_data(token, stock_code, brk_code, max_pages=800): # ⭐️ 800페이지로 상향
    url = f"{host_url}/api/dostk/stkinfo"
    all_data = []
    next_key = ""
    retry_count = 0 
    
    for i in range(max_pages): 
        headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka10052", "authorization": f"Bearer {token}"}
        if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
        
        req_data = {"mmcm_cd": brk_code, "stk_cd": stock_code, "mrkt_tp": "0", "qty_tp": "0", "pric_tp": "0", "stex_tp": "1"}
        response = requests.post(url, headers=headers, json=req_data)
        
        if response.status_code != 200:
            time.sleep(1.5) # ⭐️ 호출 제한(1700) 발생 시 잠시 휴식
            continue
            
        res_json = response.json()
        chunk = res_json.get('trde_ori_mont_trde_qty', [])
        
        if not chunk:
            retry_count += 1
            if retry_count > 2: break # 3번 연속 없으면 진짜 끝
            time.sleep(0.3)
            continue
            
        retry_count = 0
        all_data.extend(chunk)
        
        # 9시 도달 체크 (데이터가 09:00:00 이하로 내려가면 탈출)
        last_time = chunk[-1].get('tm', chunk[-1].get('stck_cntg_hour', ''))
        if last_time and last_time <= "090000":
            break
            
        next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
        if not next_key: break
        
        # ⭐️ 스캔 속도 최적화: 0.3초 -> 0.15초 (차단 안 당하는 선에서 가장 빠른 속도)
        time.sleep(0.15) 
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

lag_seconds = st.sidebar.slider("⏱️ 창구 시간 보정 (초)", 0, 180, 60)

# --- ⭐️ 원격 작업용 긴급 해결 버튼 추가 ---
st.sidebar.markdown("---")
if st.sidebar.button("🧹 오전 데이터 누락 시 (캐시 삭제)"):
    # session_state에 저장된 모든 수급 쓰레기를 비웁니다.
    for key in ['pg', 'brk1', 'brk2']:
        if key in st.session_state.get('data_cache', {}):
            st.session_state['data_cache'][key] = []
    # 검색 기록도 지워야 처음부터 다시 긁습니다.
    if 'last_search_key' in st.session_state:
        del st.session_state['last_search_key']
    st.rerun()
# -----------------------------------------

# ⭐️ 자동 검색할 10대 엘리트 창구 풀 생성
elite_keywords = ["신한", "모건", "제이피", "골드만", "메릴린치", "삼성", "한국", "미래", "NH", "KB"]
elite_brokers = {name: code for name, code in broker_dict.items() if any(k in name for k in elite_keywords)}

if st.sidebar.button("🚀 1:1 분석 & 주포 자동 스캔"):
    with st.spinner("초고속 데이터 수집 및 주포 분석 중입니다..."):
        
        # --- [내부 함수] 1. 프로그램 데이터 수집 & 파싱 ---
        def get_historical_program_data(token, stock_code, target_date, max_pages=800):
            url = f"{host_url}/api/dostk/mrkcond"
            all_data, next_key, retry_count = [], "", 0
            for i in range(max_pages):
                headers = {"Content-Type": "application/json;charset=UTF-8", "api-id": "ka90008", "authorization": f"Bearer {token}"}
                if next_key: headers.update({"cont-yn": "Y", "tr-cont": "Y", "next-key": next_key, "tr-cont-key": next_key})
                response = requests.post(url, headers=headers, json={"amt_qty_tp": "2", "stk_cd": stock_code, "date": target_date})
                if response.status_code != 200:
                    time.sleep(1.5); continue
                chunk = response.json().get('stk_tm_prm_trde_trnsn', [])
                if not chunk:
                    retry_count += 1
                    if retry_count > 2: break
                    time.sleep(0.3); continue
                retry_count = 0
                all_data.extend(chunk)
                last_time = chunk[-1].get('tm', '')
                if last_time and last_time <= "090000": break
                next_key = response.headers.get('next-key', response.headers.get('tr-cont-key', ''))
                if not next_key: break
                time.sleep(0.15)
            return all_data

        def process_pg_data(raw_data):
            if not raw_data: return pd.DataFrame(columns=['Datetime', 'Net_1m_pg', 'Cum_Net_pg']).set_index('Datetime')
            df_pg = pd.DataFrame(raw_data)
            df_pg['Datetime'] = pd.to_datetime(target_date_str + df_pg['tm'], format='%Y%m%d%H%M%S').dt.floor('min')
            df_pg['Cum_Buy'] = pd.to_numeric(df_pg['prm_buy_qty'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
            df_pg['Cum_Sell'] = pd.to_numeric(df_pg['prm_sell_qty'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
            df_pg = df_pg.sort_values('Datetime').groupby('Datetime').agg({'Cum_Buy': 'last', 'Cum_Sell': 'last'})
            df_pg['Buy_1m'] = df_pg['Cum_Buy'].diff().fillna(df_pg['Cum_Buy']).clip(lower=0)
            df_pg['Sell_1m'] = df_pg['Cum_Sell'].diff().fillna(df_pg['Cum_Sell']).clip(lower=0)
            df_pg['Net_1m_pg'] = df_pg['Buy_1m'] - df_pg['Sell_1m']
            df_pg['Cum_Net_pg'] = df_pg['Cum_Buy'] - df_pg['Cum_Sell']
            return df_pg[['Net_1m_pg', 'Cum_Net_pg']]

        # --- [내부 함수] 2. 거래원(증권사) 데이터 수집 & 파싱 ---
        def process_broker_data(raw_data, lag_sec, suffix):
            if not raw_data: return pd.DataFrame(columns=['Datetime', f'Buy_1m_{suffix}', f'Sell_1m_{suffix}', f'Cum_Net_{suffix}']).set_index('Datetime')
            df_b = pd.DataFrame(raw_data)
            time_col_b = 'tm' if 'tm' in df_b.columns else 'stck_cntg_hour'
            df_b['Datetime'] = (pd.to_datetime(target_date_str + df_b[time_col_b], format='%Y%m%d%H%M%S', errors='coerce') - pd.Timedelta(seconds=lag_sec)).dt.floor('min')
            def parse_volume(row):
                tp_str, qty_str = str(row.get('tp', '')), str(row.get('mont_trde_qty', '0')).replace(',', '')
                qty = int(qty_str.replace('-', '').replace('+', '')) if qty_str else 0
                return (0, qty) if '-' in qty_str or '매도' in tp_str else (qty, 0)
            df_b[['Buy_Vol', 'Sell_Vol']] = df_b.apply(parse_volume, axis=1, result_type='expand')
            df_b['Net_Raw'] = pd.to_numeric(df_b['acc_netprps'].astype(str).str.replace('+', '', regex=False).str.replace(',', '', regex=False), errors='coerce').fillna(0).astype(int) if 'acc_netprps' in df_b.columns else 0
            return df_b.groupby('Datetime').agg({'Buy_Vol': 'sum', 'Sell_Vol': 'sum', 'Net_Raw': 'last'}).rename(columns={'Buy_Vol': f'Buy_1m_{suffix}', 'Sell_Vol': f'Sell_1m_{suffix}', 'Net_Raw': f'Cum_Net_{suffix}'})

        # --- [데이터 병합 준비] PG & 사용자기준(키움 등) 기초 데이터 세팅 ---
        pg_raw = get_historical_program_data(auth_token, stock_number, target_date_str)
        df_pg = process_pg_data(pg_raw)
        
        brk_raw1 = get_historical_broker_data(auth_token, stock_number, target_broker_code1)
        df1 = process_broker_data(brk_raw1, lag_seconds, 'brk1')
        df1['Net_1m_brk1'] = df1['Buy_1m_brk1'] - df1['Sell_1m_brk1']

        # ⭐️ 유령 창구 필터: 기준 창구(키움)의 '거래 없는 분(0)' 횟수 측정
        kiwoom_zero_count = ((df1['Buy_1m_brk1'] == 0) & (df1['Sell_1m_brk1'] == 0)).sum()

        # --- [본격 스캔] 10대 창구 전수 조사 ---
        st.subheader("🎯 엘리트 10대 창구 정밀 스캔 진행 중...")
        progress_bar = st.progress(0)
        
        pg_scan_results = []   # 1번: 프로그램 일치(정상관) 랭킹용
        brk1_scan_results = [] # 2번: 키움 역상관(반대매매) 랭킹용
        
        elite_items = list(elite_brokers.items())
        for idx, (c_name, c_code) in enumerate(elite_items):
            progress_bar.progress((idx + 1) / len(elite_items), text=f"스캔 중: {c_name}")
            if c_code == target_broker_code1: continue
                
            c_raw = get_historical_broker_data(auth_token, stock_number, c_code, max_pages=80) 
            df_c = process_broker_data(c_raw, lag_seconds, 'cand')
            df_c['Net_1m_cand'] = df_c['Buy_1m_cand'] - df_c['Sell_1m_cand']
            
            # ⭐️ 유령 창구 필터 적용
            cand_zero_count = ((df_c['Buy_1m_cand'] == 0) & (df_c['Sell_1m_cand'] == 0)).sum()
            if cand_zero_count > (kiwoom_zero_count * 2):
                continue # 거래가 너무 없으면 알고리즘/주포에서 탈락!
            
            # 1. PG 상관성 계산 (df_pg 존재 시)
            if not df_pg.empty:
                df_pg_c = df_pg.join(df_c, how='outer').fillna(0)
                active_pg = df_pg_c[(df_pg_c['Net_1m_pg'] != 0) | (df_pg_c['Net_1m_cand'] != 0)]
                if len(active_pg) >= 3:
                    corr_pg = active_pg['Net_1m_pg'].corr(active_pg['Net_1m_cand'])
                    if not pd.isna(corr_pg):
                        pg_scan_results.append({'name': c_name, 'corr': corr_pg, 'df': df_pg_c})
                        
            # 2. 기준창구(키움) 상관성 계산
            df_b1_c = df1.join(df_c, how='outer').fillna(0)
            active_b1 = df_b1_c[(df_b1_c['Net_1m_brk1'] != 0) | (df_b1_c['Net_1m_cand'] != 0)]
            if len(active_b1) >= 3:
                corr_b1 = active_b1['Net_1m_brk1'].corr(active_b1['Net_1m_cand'])
                if not pd.isna(corr_b1):
                    brk1_scan_results.append({'name': c_name, 'corr': corr_b1, 'df': df_b1_c})

        progress_bar.empty()

        # ========================================================================
        # 👑 [RANK 1] 프로그램(알고리즘) 본체 추적 보드 (정상관 높은 순)
        # ========================================================================
        pg_scan_results.sort(key=lambda x: x['corr'], reverse=True) # 일치해야 하므로 내림차순(+)
        top_5_pg = pg_scan_results[:5]

        st.markdown("---")
        st.markdown("### 🤖 [TRACK 1] 프로그램(PG) 알고리즘 통로 추적 TOP 5")
        st.caption("프로그램 매매와 가장 똑같이 움직이는 창구를 찾습니다. (상관계수 +0.5 이상이면 알고리즘 본체일 확률 90%)")

        if top_5_pg:
            cols = st.columns(5)
            for rank, (col, res) in enumerate(zip(cols, top_5_pg), 1):
                with col:
                    # 일치할수록 빨간색(주포 발견)
                    box_color = "#ff4d4d" if res['corr'] >= 0.5 else "#ff9999" if res['corr'] >= 0.3 else "#abb8c3"
                    st.markdown(f"""
                        <div style='padding: 10px; border-radius: 8px; border: 2px solid {box_color}; background-color: white; text-align: center; height: 160px;'>
                            <p style='margin:0; font-size: 14px; font-weight: bold; color: {box_color};'>{rank}위</p>
                            <p style='margin: 5px 0; font-size: 15px; font-weight: bold; color: #333;'>{res['name']}</p>
                            <h2 style='margin: 10px 0; color: {box_color};'>{res['corr']:+.2f}</h2>
                            <p style='margin:0; font-size: 11px; color: gray;'>{'알고리즘 유력' if res['corr'] >= 0.5 else '관망'}</p>
                        </div>
                        """, unsafe_allow_html=True)
            
            # PG 1위 차트
            best_pg = top_5_pg[0]
            if best_pg['corr'] >= 0.3:
                st.success(f"🎯 알고리즘 포착! 오늘 프로그램 매매의 핵심 통로는 [{best_pg['name']}] 창구로 강하게 의심됩니다.")
                b_df = best_pg['df']
                b_df['Cum_Net_cand'] = b_df['Net_1m_cand'].cumsum()
                fig_pg = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08, row_heights=[0.6, 0.4])
                fig_pg.add_trace(go.Scatter(x=b_df.index, y=b_df['Cum_Net_pg'], mode='lines', name="프로그램 누적", line=dict(color='black', width=3, dash='dash')), row=1, col=1)
                fig_pg.add_trace(go.Scatter(x=b_df.index, y=b_df['Cum_Net_cand'], mode='lines', name=best_pg['name'], line=dict(color='#ff4d4d', width=3)), row=1, col=1)
                fig_pg.add_trace(go.Bar(x=b_df.index, y=b_df['Net_1m_pg'], name="PG 1분", marker_color='gray', opacity=0.5), row=2, col=1)
                fig_pg.add_trace(go.Bar(x=b_df.index, y=b_df['Net_1m_cand'], name=best_pg['name'], marker_color='#ff4d4d', opacity=0.8), row=2, col=1)
                fig_pg.update_layout(height=450, template='plotly_white', barmode='group', showlegend=False, title_text=f"프로그램 누적매수 vs {best_pg['name']} 누적매수")
                st.plotly_chart(fig_pg, use_container_width=True)
        else:
            st.info("프로그램 데이터가 없거나 뚜렷한 일치 창구가 없습니다.")


        # ========================================================================
        # 👑 [RANK 2] 기준 창구(키움) 반대매매 추적 보드 (역상관 낮은 순)
        # ========================================================================
        brk1_scan_results.sort(key=lambda x: x['corr']) # 반대여야 하므로 오름차순(-)
        top_5_brk = brk1_scan_results[:5]

        st.markdown("---")
        st.markdown(f"### 🐜 [TRACK 2] {selected_broker_name1} 반대매매(역상관) 추적 TOP 5")
        st.caption("개미 창구와 정반대로 움직이며 물량을 받아먹거나 던지는 진짜 주포를 찾습니다.")

        if top_5_brk:
            cols = st.columns(5)
            for rank, (col, res) in enumerate(zip(cols, top_5_brk), 1):
                with col:
                    # 역상관일수록 파란색(반대 세력 발견)
                    box_color = "#0066ff" if res['corr'] <= -0.5 else "#5e96ff" if res['corr'] <= -0.3 else "#abb8c3"
                    st.markdown(f"""
                        <div style='padding: 10px; border-radius: 8px; border: 2px solid {box_color}; background-color: white; text-align: center; height: 160px;'>
                            <p style='margin:0; font-size: 14px; font-weight: bold; color: {box_color};'>{rank}위</p>
                            <p style='margin: 5px 0; font-size: 15px; font-weight: bold; color: #333;'>{res['name']}</p>
                            <h2 style='margin: 10px 0; color: {box_color};'>{res['corr']:+.2f}</h2>
                            <p style='margin:0; font-size: 11px; color: gray;'>{'세력 감지' if res['corr'] <= -0.3 else '관망'}</p>
                        </div>
                        """, unsafe_allow_html=True)
            
            # 키움 역상관 1위 차트
            best_brk = top_5_brk[0]
            if best_brk['corr'] <= -0.3:
                st.success(f"🥇 역상관 포착: [{best_brk['name']}]의 누적 순매수와 [{selected_broker_name1}]은 완벽하게 거꾸로 달리고 있습니다.")
                b_df = best_brk['df']
                b_df['Cum_Net_cand'], b_df['Cum_Net_brk1'] = b_df['Net_1m_cand'].cumsum(), b_df['Net_1m_brk1'].cumsum()
                fig_brk = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08, row_heights=[0.6, 0.4])
                fig_brk.add_trace(go.Scatter(x=b_df.index, y=b_df['Cum_Net_brk1'], mode='lines', name=selected_broker_name1, line=dict(color='#ff4d4d', width=3)), row=1, col=1)
                fig_brk.add_trace(go.Scatter(x=b_df.index, y=b_df['Cum_Net_cand'], mode='lines', name=best_brk['name'], line=dict(color='#0066ff', width=3)), row=1, col=1)
                fig_brk.add_trace(go.Bar(x=b_df.index, y=b_df['Net_1m_brk1'], name=selected_broker_name1, marker_color='#ff4d4d', opacity=0.7), row=2, col=1)
                fig_brk.add_trace(go.Bar(x=b_df.index, y=b_df['Net_1m_cand'], name=best_brk['name'], marker_color='#0066ff', opacity=0.7), row=2, col=1)
                fig_brk.update_layout(height=450, template='plotly_white', barmode='group', showlegend=False, title_text=f"{selected_broker_name1} 누적매수 vs {best_brk['name']} 누적매수")
                st.plotly_chart(fig_brk, use_container_width=True)
        else:
            st.info("뚜렷한 역상관을 보이는 메이저 세력 창구가 발견되지 않았습니다.")
