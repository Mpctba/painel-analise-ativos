# admin_app.py
# Painel de Análise de Ativos com suporte, resistência, colunas K e previsão (sem Beta para performance)

import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import datetime
from pivo import calculate_pivot_points
from streamlit_autorefresh import st_autorefresh
import numpy as np
from pandas.tseries.offsets import Week

st.set_page_config(layout="wide", page_title="Painel de Análise de Ativos")

EXCEL_PATH = "BOV2025_Analise_Completa_B.xlsx"

@st.cache_data(ttl=60)
def buscar_variacoes_ibov_ouro_dolar_selic_vix():
    try:
        ibov = yf.download("^BVSP", period="2d")['Close']
        ouro = yf.download("GC=F", period="2d")['Close']
        dolar = yf.download("USDBRL=X", period="2d")['Close']
        vix = yf.download("^VIX", period="2d")['Close']

        var_ibov = float(((ibov.iloc[-1] / ibov.iloc[-2]) - 1) * 100)
        var_ouro = float(((ouro.iloc[-1] / ouro.iloc[-2]) - 1) * 100)
        var_dolar = float(((dolar.iloc[-1] / dolar.iloc[-2]) - 1) * 100)
        var_vix = float(((vix.iloc[-1] / vix.iloc[-2]) - 1) * 100)

        return var_ibov, var_ouro, var_dolar, var_vix
    except:
        return None, None, None, None

def style_weekly_gains(df):
    styler_df = pd.DataFrame('', index=df.index, columns=df.columns)
    date_cols = sorted([col for col in df.columns if isinstance(col, str) and '/' in str(col)],
                       key=lambda d: datetime.strptime(d, '%d/%m/%Y'))
    if not date_cols:
        return styler_df
    for idx in df.index:
        for i in range(1, len(date_cols)):
            prev_col = date_cols[i-1]
            current_col = date_cols[i]
            prev_val = df.loc[idx, prev_col]
            current_val = df.loc[idx, current_col]
            if pd.notna(current_val) and pd.notna(prev_val):
                if current_val > prev_val:
                    styler_df.loc[idx, current_col] = 'color: #2E7D32;'
                elif current_val < prev_val:
                    styler_df.loc[idx, current_col] = 'color: #C62828;'
    return styler_df

@st.cache_data(ttl=60)
def buscar_e_calcular_tudo(tickers_list):
    if not tickers_list:
        return pd.DataFrame()

    tickers_sa = [f"{t}.SA" if '.' not in t else t for t in tickers_list]
    start_date = "2024-05-01"
    end_date = datetime.now()

    with st.spinner(f"A buscar dados de mercado para {len(tickers_list)} tickers..."):
        hist_data = yf.download(tickers_sa, start=start_date, end=end_date, progress=False)
        if hist_data.empty:
            st.error("Não foi possível obter dados históricos para o período especificado.")
            return pd.DataFrame()

    results = []
    for ticker in tickers_list:
        try:
            ticker_sa = f"{ticker}.SA" if '.' not in ticker else ticker
            ticker_hist = hist_data.loc[:, (slice(None), ticker_sa)]
            ticker_hist.columns = ticker_hist.columns.droplevel(1)
            ticker_hist.dropna(how='all', inplace=True)

            if ticker_hist.empty or len(ticker_hist) < 2:
                continue

            latest_close = ticker_hist['Close'].iloc[-1]
            previous_close = ticker_hist['Close'].iloc[-2]
            var_diaria = ((latest_close / previous_close) - 1) * 100

            pivot_points = calculate_pivot_points(ticker_hist)
            suporte_imediato = pd.NA
            resistencia_imediata = pd.NA
            amplitude_pct = pd.NA
            k2 = k3 = k5 = k9 = k17 = k33 = k64 = pd.NA
            kp2 = kp3 = kp5 = kp9 = kp17 = kp33 = kp64 = pd.NA
            if pivot_points:
                suportes = [pivot_points.get(k) for k in ['P', 'S1', 'S2', 'S3'] if pivot_points.get(k) is not None and pivot_points[k] < latest_close]
                if suportes:
                    suporte_imediato = max(suportes)
                resistencias = [pivot_points.get(k) for k in ['P', 'R1', 'R2', 'R3'] if pivot_points.get(k) is not None and pivot_points[k] > latest_close]
                if resistencias:
                    resistencia_imediata = min(resistencias)
                if pd.notna(suporte_imediato) and pd.notna(resistencia_imediata) and suporte_imediato > 0:
                    amplitude_pct = ((resistencia_imediata / suporte_imediato) - 1) * 100
                    k2 = amplitude_pct / -2
                    k3 = amplitude_pct / -3
                    k5 = amplitude_pct / -5
                    k9 = amplitude_pct / -9
                    k17 = amplitude_pct / -17
                    k33 = amplitude_pct / -33
                    k64 = amplitude_pct / -64
                    kp2 = amplitude_pct / 2
                    kp3 = amplitude_pct / 3
                    kp5 = amplitude_pct / 5
                    kp9 = amplitude_pct / 9
                    kp17 = amplitude_pct / 17
                    kp33 = amplitude_pct / 33
                    kp64 = amplitude_pct / 64

            weekly_closes = ticker_hist['Close'].resample('W-FRI').last().dropna()
            friday_prices = {date.strftime('%d/%m/%Y'): price for date, price in weekly_closes.items()}

            try:
                if len(weekly_closes) >= 4:
                    last_4 = weekly_closes[-4:]
                    datas_x = [date.timestamp() for date in last_4.index]
                    valores_y = list(last_4.values)
                    coef = np.polyfit(datas_x, valores_y, deg=1)
                    proxima_sexta = (datetime.now() + Week(weekday=4)).replace(hour=0, minute=0, second=0, microsecond=0)
                    x_prev = proxima_sexta.timestamp()
                    previsao = np.polyval(coef, x_prev)
                    std_dev = np.std(valores_y)
                    previsao_menos = previsao - std_dev
                    previsao_mais = previsao + std_dev
                else:
                    previsao = previsao_menos = previsao_mais = pd.NA
            except:
                previsao = previsao_menos = previsao_mais = pd.NA

            todos_ks = [k2, k3, k5, k9, k17, k33, k64, kp2, kp3, kp5, kp9, kp17, kp33, kp64]
            k_suporte = sorted([k for k in todos_ks if pd.notna(k) and k < var_diaria], reverse=True)[0] if any(pd.notna(k) and k < var_diaria for k in todos_ks) else pd.NA
            k_resistencia = sorted([k for k in todos_ks if pd.notna(k) and k > var_diaria])[0] if any(pd.notna(k) and k > var_diaria for k in todos_ks) else pd.NA

            ticker_data = {
                'Ticker': ticker,
                'Cotação Atual (R$)': latest_close,
                'Var (%) Diária': var_diaria,
                **friday_prices,
                **pivot_points,
                'Suporte Imediato': suporte_imediato,
                'Resistência Imediata': resistencia_imediata,
                'Amplitude (%)': amplitude_pct,
                'K(-2)': k2, 'K(-3)': k3, 'K(-5)': k5, 'K(-9)': k9, 'K(-17)': k17, 'K(-33)': k33, 'K(-64)': k64,
                'K(+2)': kp2, 'K(+3)': kp3, 'K(+5)': kp5, 'K(+9)': kp9, 'K(+17)': kp17, 'K(+33)': kp33, 'K(+64)': kp64,
                'K Suporte': k_suporte,
                'K Resistência': k_resistencia,
                'Spread (%)': (k_resistencia - k_suporte) if pd.notna(k_resistencia) and pd.notna(k_suporte) else pd.NA,
                'Previsão (-)': previsao_menos,
                'Previsão': previsao,
                'Previsão (+)': previsao_mais
            }
            results.append(ticker_data)
        except (KeyError, IndexError):
            continue

    st.success("Busca e análise concluídas!")
    df_final = pd.DataFrame(results)
    if df_final.empty:
        st.warning("Não foram encontrados dados para nenhum dos tickers solicitados.")
    return df_final

if 'analysis_df' not in st.session_state:
    st.session_state.analysis_df = pd.DataFrame()

var_ibov, var_ouro, var_dolar, var_vix = buscar_variacoes_ibov_ouro_dolar_selic_vix()

try:
    df_tickers = pd.read_excel(EXCEL_PATH, sheet_name="Streamlit")
    if 'Ticker' in df_tickers.columns:
        tickers = df_tickers['Ticker'].dropna().unique().tolist()

        if var_ibov is not None and var_ouro is not None and var_dolar is not None and var_vix is not None:
            ibov_color = 'green' if var_ibov >= 0 else 'red'
            ouro_color = 'green' if var_ouro >= 0 else 'red'
            dolar_color = 'green' if var_dolar >= 0 else 'red'
            vix_color = 'red' if var_vix >= 0 else 'green'
            vix_label = '(Mercado pessimista)' if var_vix >= 0 else '(Mercado otimista)'
            st.sidebar.markdown(f"""
                <div style='font-size:18px; font-weight:bold;'>
                    📊 IBOV: <span style='color:{ibov_color};'>{var_ibov:.2f}%</span> |
                    Ouro: <span style='color:{ouro_color};'>{var_ouro:.2f}%</span> |
                    Dólar: <span style='color:{dolar_color};'>{var_dolar:.2f}%</span> |
                    VIX: <span style='color:{vix_color};'>{var_vix:.2f}% <span style='font-weight:normal; color:{vix_color};'>{vix_label}</span></span>
                </div>""", unsafe_allow_html=True)

        auto_update = st.sidebar.toggle("🔄 Ligar atualização automática (1 min)")

        if auto_update:
            st_autorefresh(interval=60 * 1000, key="auto_refresher")
            st.session_state.analysis_df = buscar_e_calcular_tudo(tickers)
        elif st.sidebar.button("📈 Analisar Ativos Manualmente", type="primary"):
            st.session_state.analysis_df = buscar_e_calcular_tudo(tickers)
    else:
        st.sidebar.error("❌ A planilha não contém a coluna 'Ticker'.")
except Exception as e:
    st.sidebar.error(f"⚠️ Erro ao carregar a planilha automática: {e}")

if 'analysis_df' in st.session_state and not st.session_state.analysis_df.empty:
    st.write("---")
    st.write("### Análise de Ativos")

    all_tickers = st.session_state.analysis_df['Ticker'].unique()
    selected_tickers = st.multiselect("Filtrar por Ativo:", options=all_tickers, default=None, placeholder="Selecione um ou mais ativos para visualizar")
    df_to_display = st.session_state.analysis_df[st.session_state.analysis_df['Ticker'].isin(selected_tickers)] if selected_tickers else st.session_state.analysis_df

    try:
        st.dataframe(df_to_display.style.format({
            'Previsão (-)': 'R$ {:,.2f}', 'Previsão (+)': 'R$ {:,.2f}',
            'Var (%) Diária': '{:,.2f}%','Amplitude (%)': '{:,.2f}%','Spread (%)': '{:,.2f}%','Previsão': 'R$ {:,.2f}','Cotação Atual (R$)': 'R$ {:,.2f}',
            'K(-2)': '{:,.2f}%', 'K(-3)': '{:,.2f}%', 'K(-5)': '{:,.2f}%', 'K(-9)': '{:,.2f}%', 'K(-17)': '{:,.2f}%', 'K(-33)': '{:,.2f}%', 'K(-64)': '{:,.2f}%','K(+2)': '{:,.2f}%', 'K(+3)': '{:,.2f}%', 'K(+5)': '{:,.2f}%', 'K(+9)': '{:,.2f}%', 'K(+17)': '{:,.2f}%', 'K(+33)': '{:,.2f}%', 'K(+64)': '{:,.2f}%','K Suporte': '{:,.2f}%', 'K Resistência': '{:,.2f}%'
        }, na_rep='-'), use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao exibir dados: {e}")
        st.write(st.session_state.analysis_df)
else:
    st.info("Aguardando carregamento dos dados.")
