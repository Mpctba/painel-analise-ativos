import os
import streamlit as st
import pandas as pd
import yfinance as yf
import unicodedata
import numpy as np
from datetime import date, timedelta, datetime as dt
import pytz
from scipy.fft import fft, fftfreq
from statsmodels.tsa.stattools import acf
from scipy.optimize import curve_fit
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# --- Configurações Iniciais da Aplicação ---
st.set_page_config(page_title="📈 Análise de Preços Semanais - BOV2025", layout="wide")
st.title("📈 Análise de Preços Semanais - BOV2025")

EXCEL_PATH = "BOV2025_Analise_Completa_B.xlsx"
SHEET_NAME_STOCKS = "Streamlit" # Aba para ações da B3
SHEET_NAME_CRYPTO = "Criptos" # Aba para criptomoedas
SHEET_NAME_ETFS = "ETF" # Aba para ETFs
SHEET_NAME_FIIS = "FII" # Nova aba para FIIs
SHEET_NAME_BDRS = "BDR" # Nova aba para BDRs

HIDDEN_FILES = ["hidden_cols.txt", "hidden_col.txt"]
DEFAULT_TICKERS_FILE = "default_tickers.txt"

# --- Funções de Carregamento e Busca de Dados ---

@st.cache_data(ttl=300)
def carregar_planilha(path: str, aba: str) -> pd.DataFrame:
    """Carrega os dados de uma planilha Excel e normaliza os nomes das colunas."""
    try:
        df = pd.read_excel(path, sheet_name=aba)
        df.columns = [unicodedata.normalize('NFC', str(col).strip()) for col in df.columns]
        df = df.dropna(axis=1, how="all")
        df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
        return df
    except FileNotFoundError:
        st.error(f"O arquivo Excel '{path}' não foi encontrado.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao ler a aba '{aba}' do Excel: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_price_var_min_max_last(ticker_yf: str):
    """Busca dados de cotação usando yfinance."""
    try:
        ticker_data = yf.Ticker(ticker_yf)
        hist = ticker_data.history(period="1y") # Pega 1 ano para cálculos gerais
        if hist.empty: return None, None, None, None, None, pd.DataFrame()

        # Calcula a cotação atual e a variação diária
        recent = hist.tail(2)
        close_today, var = (round(recent["Close"].iloc[-1], 2), round(((recent["Close"].iloc[-1] - recent["Close"].iloc[-2]) / recent["Close"].iloc[-2]) * 100, 2)) if len(recent) >= 2 else (round(recent["Close"].iloc[-1], 2) if len(recent) == 1 else None, None)

        # Filtra os dados apenas para sextas-feiras desde jun/24
        hist_jun = hist[hist.index.date >= date(2024, 6, 1)]
        sextas = hist_jun[hist_jun.index.weekday == 4]
        min_sexta = round(sextas["Close"].min(), 2) if not sextas.empty else None
        max_sexta = round(sextas["Close"].max(), 2) if not sextas.empty else None
        fechamento_mais_recente_sexta = round(sextas["Close"].iloc[-1], 2) if not sextas.empty else None

        return close_today, var, min_sexta, max_sexta, fechamento_mais_recente_sexta, hist
    except Exception as e:
        # st.warning(f"Erro yfinance para {ticker_yf}: {e}")
        return None, None, None, None, None, pd.DataFrame()

@st.cache_data(ttl=600)
def get_index_data(ticker_yf: str):
    """Busca dados de preço atual e variação diária para um índice."""
    try:
        hist = yf.Ticker(ticker_yf).history(period="2d")
        if hist.empty: return None, None
        close_today = round(hist["Close"].iloc[-1], 2)
        var = round(((close_today - hist["Close"].iloc[-2]) / hist["Close"].iloc[-2]) * 100, 2) if len(hist) >= 2 and hist["Close"].iloc[-2] != 0 else None
        return close_today, var
    except Exception:
        return None, None

# --- Funções de Cálculo e Análise ---

def calcular_sr(row):
    H, L, C = row.get("Máxima sexta desde jun/24"), row.get("Mínima sexta desde jun/24"), row.get("Fechamento mais recente")
    if pd.notnull(H) and pd.notnull(L) and pd.notnull(C):
        P = (H + L + C) / 3
        return pd.Series([round(L - 2*(H-P), 2), round(P-(H-L), 2), round(2*P-H, 2), round(P, 2), round(2*P-L, 2), round(P+(H-L), 2), round(H+2*(P-L), 2)])
    return pd.Series([None]*7)

def encontrar_valores_proximos(row):
    preco = row.get("Cotação atual")
    niveis = sorted([v for k, v in row.items() if k in ["S3","S2","S1","P","R1","R2","R3"] and pd.notnull(v)])
    abaixo = max([v for v in niveis if v<=preco], default=None)
    acima = min([v for v in niveis if v>preco], default=None)
    return pd.Series([abaixo, acima])

def calcular_distancia_percentual(row):
    preco, abaixo, acima = row.get("Cotação atual"), row.get("Nível abaixo"), row.get("Nível acima")
    if preco is None or preco == 0: return None
    d1 = abs((preco-abaixo)/preco)*100 if pd.notnull(abaixo) else np.inf
    d2 = abs((acima-preco)/preco)*100 if pd.notnull(acima) else np.inf
    min_dist = min(d1, d2)
    return round(min_dist, 2) if min_dist != np.inf else None

def encontrar_var_faixa(row, k_values_list):
    var = row.get("Var")
    arr = sorted([v for v in k_values_list if pd.notnull(v)])
    if pd.notnull(var) and arr:
        abaixo = max([v for v in arr if v <= var], default=None)
        acima = min([v for v in arr if v > var], default=None)
        return pd.Series([abaixo, acima])
    return pd.Series([None, None])

def prever_alvo(row, last_cols, last_dates, next_friday):
    ys = [row.get(c) for c in last_cols]
    valid_points = [(last_dates[i].toordinal(), y) for i, y in enumerate(ys) if pd.notnull(y)]
    if len(valid_points) < 2: return None
    valid_xs, valid_ys = zip(*valid_points)
    m, b = np.polyfit(valid_xs, valid_ys, 1)
    return round(m * next_friday.toordinal() + b, 2)

def highlight_colunas_comparadas(row, colunas_para_estilo):
    vals = row[colunas_para_estilo].values
    styles = [''] * len(vals)
    for i in range(1, len(vals)):
        ant, atual = vals[i-1], vals[i]
        if pd.notnull(ant) and pd.notnull(atual):
            if atual >= ant: styles[i] = 'color: green; font-weight: bold'
            else: styles[i] = 'color: red; font-weight: bold'
    return styles
    
def highlight_insights(val):
    if isinstance(val, str):
        if "Compra" in val: return 'color: green; font-weight: bold'
        if "Evitar" in val: return 'color: red; font-weight: bold'
    return ''

def calculate_consecutive_growth(row, static_date_cols, current_quote_col):
    all_values = [row.get(c) for c in static_date_cols]
    if pd.notnull(row.get(current_quote_col)) and len(all_values) > 0 and pd.notnull(all_values[-1]) and row.get(current_quote_col) > all_values[-1]:
        all_values.append(row.get(current_quote_col))
    
    count = 0
    for i in range(len(all_values) - 1, 0, -1):
        if pd.notnull(all_values[i]) and pd.notnull(all_values[i-1]) and all_values[i] > all_values[i-1]:
            count += 1
        else:
            break
    return count

def load_default_tickers(file_path: str, all_options: list) -> list:
    if not os.path.exists(file_path): return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            loaded = [unicodedata.normalize('NFC', line.strip()) for line in f if line.strip()]
            return [t for t in loaded if t in all_options]
    except Exception as e:
        st.warning(f"Erro ao carregar tickers padrão: {e}")
        return []

def calculate_additional_indicators(hist_data: pd.DataFrame) -> pd.Series:
    if hist_data.empty or len(hist_data) < 20:
        return pd.Series({k: None for k in ['Volatilidade_Anualizada', 'SMA_20_Dias', 'EMA_20_Dias', 'BB_Upper', 'BB_Lower', 'RSI_14_Dias', 'Volume_Medio_20_Dias', 'Momentum_10_Dias', 'Aceleracao_10_Dias']})
    hist_data = hist_data.copy().sort_index()
    close = hist_data['Close']
    log_returns = np.log(close / close.shift(1))
    vol = log_returns.rolling(window=20).std().iloc[-1] * np.sqrt(252) * 100
    sma_20 = close.rolling(window=20).mean().iloc[-1]
    ema_20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
    std_dev_20 = close.rolling(window=20).std().iloc[-1]
    bb_upper = sma_20 + (std_dev_20 * 2)
    bb_lower = sma_20 - (std_dev_20 * 2)
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] != 0 else np.inf
    rsi_14 = 100 - (100 / (1 + rs))
    vol_med = hist_data['Volume'].rolling(window=20).mean().iloc[-1]
    mom = ((close.iloc[-1] - close.iloc[-11]) / close.iloc[-11]) * 100 if len(close) > 10 and close.iloc[-11] != 0 else None
    accel = close.pct_change().iloc[-10:].mean() * 100 if len(close) > 10 else None
    return pd.Series({
        'Volatilidade_Anualizada': round(vol, 2) if pd.notnull(vol) else None,
        'SMA_20_Dias': round(sma_20, 2) if pd.notnull(sma_20) else None,
        'EMA_20_Dias': round(ema_20, 2) if pd.notnull(ema_20) else None,
        'BB_Upper': round(bb_upper, 2) if pd.notnull(bb_upper) else None,
        'BB_Lower': round(bb_lower, 2) if pd.notnull(bb_lower) else None,
        'RSI_14_Dias': round(rsi_14, 2) if pd.notnull(rsi_14) else None,
        'Volume_Medio_20_Dias': round(vol_med) if pd.notnull(vol_med) else None,
        'Momentum_10_Dias': round(mom, 2) if pd.notnull(mom) else None,
        'Aceleracao_10_Dias': round(accel, 2) if pd.notnull(accel) else None,
    })

# --- FUNÇÃO DE SCORE CORRIGIDA ---
def calculate_attractiveness_score(row: pd.Series, weights: dict) -> float:
    """Calcula uma pontuação de atratividade de 0 a 10 para um ticker."""
    score = 0.0

    # 1. Volatilidade Anualizada (menor é melhor)
    vol = row.get('Volatilidade_Anualizada')
    if pd.notnull(vol):
        if vol < 20: score += 1.5 * weights.get('volatility', 1.0)
        elif 20 <= vol < 40: score += 0.5 * weights.get('volatility', 1.0)

    # 2. Preço vs. Médias Móveis (tendência de alta)
    close_price, sma_20, ema_20 = row.get('Cotação atual'), row.get('SMA_20_Dias'), row.get('EMA_20_Dias')
    if pd.notnull(close_price) and pd.notnull(sma_20) and close_price > sma_20:
        score += 1.5 * weights.get('moving_averages', 1.0)
    if pd.notnull(close_price) and pd.notnull(ema_20) and close_price > ema_20:
        score += 1.5 * weights.get('moving_averages', 1.0)

    # 3. RSI (condição de sobrevenda)
    rsi = row.get('RSI_14_Dias')
    if pd.notnull(rsi):
        if rsi < 30: score += 2.5 * weights.get('rsi', 1.0)
        elif 30 <= rsi <= 70: score += 0.5 * weights.get('rsi', 1.0)

    # 4. Bandas de Bollinger (preço próximo à banda inferior)
    bb_lower, bb_upper = row.get('BB_Lower'), row.get('BB_Upper')
    if pd.notnull(close_price) and pd.notnull(bb_lower):
        if close_price < bb_lower * 1.01: score += 2.0 * weights.get('bollinger_bands', 1.0)
        elif pd.notnull(bb_upper) and close_price < bb_upper: score += 0.5 * weights.get('bollinger_bands', 1.0)

    # 5. Momentum e Aceleração
    momentum, aceleracao = row.get('Momentum_10_Dias'), row.get('Aceleracao_10_Dias')
    if pd.notnull(momentum) and momentum > 5: score += 1.0 * weights.get('momentum', 1.0)
    if pd.notnull(aceleracao) and aceleracao > 1.0: score += 1.0 * weights.get('acceleration', 1.0)

    # 6. Consistência de Crescimento (Nível)
    nivel = row.get('Nível')
    if pd.notnull(nivel) and nivel >= 3: score += 1.5 * weights.get('growth_consistency', 1.0)

    return round(min(10.0, score), 2)

def generate_insight(row: pd.Series) -> str:
    score = row.get("Score")
    if pd.notnull(score):
        if score >= 8.0: return "Compra Forte"
        if score >= 6.0: return "Atenção para Compra"
        if score >= 4.0: return "Monitorar"
        return "Evitar / Atenção"
    return "Sem dados para análise"

@st.cache_data(ttl=3600)
def get_dividend_data(ticker_yf: str) -> tuple[date | None, date | None]:
    try:
        actions = yf.Ticker(ticker_yf).actions
        if actions.empty or 'Dividends' not in actions.columns: return None, None
        dividends = actions[actions['Dividends'] > 0].index.tz_localize(None).date
        today = date.today()
        future_divs = sorted([d for d in dividends if d >= today])
        past_divs = sorted([d for d in dividends if d < today])
        next_div = future_divs[0] if future_divs else None
        last_div = past_divs[-1] if past_divs else None
        return next_div, last_div
    except:
        return None, None

def process_and_display_data(sheet_name: str, asset_type_display_name: str, weights: dict):
    df = carregar_planilha(EXCEL_PATH, sheet_name)
    if df.empty or "Ticker" not in df.columns:
        st.info(f"A planilha '{sheet_name}' está vazia ou não contém a coluna 'Ticker'.")
        return

    # --- Processamento de Dados ---
    df["Ticker_YF"] = df["Ticker"].astype(str).str.strip()
    if asset_type_display_name in ["Ações", "ETFs", "FIIs", "BDRs"]:
        df["Ticker_YF"] += ".SA"

    data_yf = df["Ticker_YF"].apply(lambda t: pd.Series(get_price_var_min_max_last(t), index=["Cotação atual", "Var", "Mínima sexta desde jun/24", "Máxima sexta desde jun/24", "Fechamento mais recente", "Raw_Hist_Data"]))
    df = pd.concat([df, data_yf], axis=1)

    df_not_na = df[df['Cotação atual'].notna()].copy()

    df_not_na[["S3","S2","S1","P","R1","R2","R3"]] = df_not_na.apply(calcular_sr, axis=1)
    df_not_na[["Nível abaixo","Nível acima"]] = df_not_na.apply(encontrar_valores_proximos, axis=1)
    df_not_na["Delta"] = df_not_na.apply(calcular_distancia_percentual, axis=1)
    df_not_na["Amplitude"] = df_not_na.apply(lambda r: ((r["Nível acima"]/r["Nível abaixo"]-1)*100) if pd.notnull(r["Nível acima"]) and pd.notnull(r["Nível abaixo"]) and r["Nível abaixo"]!=0 else None, axis=1).round(2)
    
    k_div = [-2,-3,-5,-9,-17,-33,-65,65,33,17,9,5,3,2]
    k_cols = [f"K ({k})" for k in k_div]
    for i, k in enumerate(k_div):
        df_not_na[k_cols[i]] = df_not_na["Amplitude"].apply(lambda amp: round(amp/k, 2) if pd.notnull(amp) else None)
    
    df_not_na[["Var (abaixo)","Var (acima)"]] = df_not_na.apply(lambda row: encontrar_var_faixa(row, [row[c] for c in k_cols if c in row]), axis=1)
    df_not_na["Spread (%)"] = (df_not_na["Var (acima)"] - df_not_na["Var (abaixo)"]).round(2)

    date_cols = sorted([c for c in df.columns if isinstance(c, str) and c[:4].isdigit() and c[4] == '-'])
    for c in date_cols: df_not_na[c] = pd.to_numeric(df_not_na[c], errors="coerce")
    
    if len(date_cols) >= 4:
        last_cols = date_cols[-4:]
        last_dates = [pd.to_datetime(c).date() for c in last_cols]
        next_friday = date.today() + timedelta(days=(4 - date.today().weekday() + 7) % 7)
        df_not_na['Alvo'] = df_not_na.apply(lambda row: prever_alvo(row, last_cols, last_dates, next_friday), axis=1)
    else:
        df_not_na['Alvo'] = None
    
    static_price_cols = date_cols[-5:] if len(date_cols) >= 5 else date_cols
    df_not_na['Nível'] = df_not_na.apply(lambda r: calculate_consecutive_growth(r, static_price_cols, "Cotação atual"), axis=1)
    
    indicator_data = df_not_na["Raw_Hist_Data"].apply(calculate_additional_indicators)
    df_not_na = pd.concat([df_not_na.drop(columns=indicator_data.columns, errors='ignore'), indicator_data], axis=1)
    
    df_not_na['Score'] = df_not_na.apply(lambda r: calculate_attractiveness_score(r, weights), axis=1)
    df_not_na['Insight'] = df_not_na.apply(generate_insight, axis=1)

    df_final = df.drop(columns=df_not_na.columns.intersection(df.columns).drop('Ticker', errors='ignore')).merge(df_not_na, on="Ticker", how="left")
    
    # --- Exibição de Dados ---
    all_tickers = sorted(df_final["Ticker"].unique())
    sel = st.multiselect(f"Filtrar por Ticker ({asset_type_display_name}):", options=all_tickers, default=load_default_tickers(DEFAULT_TICKERS_FILE, all_tickers))
    display_df = df_final[df_final["Ticker"].isin(sel)] if sel else df_final

    hidden_cols_base = ["Raw_Hist_Data", "Ticker_YF"] + k_cols
    hidden_cols_config = [line.strip() for fname in HIDDEN_FILES if os.path.exists(fname) for line in open(fname, 'r', encoding='utf-8')]
    cols_to_hide = [c for c in hidden_cols_base + hidden_cols_config if c in display_df.columns]
    
    base_order = ["Ticker", "Insight", "Score", "Spread (%)", "Var", "Nível"]
    price_cols = date_cols + ["Cotação atual", "Alvo"]
    final_order = [c for c in base_order + price_cols if c in display_df.columns]
    remaining_cols = [c for c in display_df.columns if c not in final_order and c not in cols_to_hide]
    display_df = display_df[final_order + remaining_cols]

    # Formatação e Estilo
    fmt = {c: "{:.2f}" for c in display_df.select_dtypes(include=[np.number]).columns}
    fmt.update({'Nível': "{:.0f}", 'Score': "{:.1f}", 'Var': "{:.2f}%", 'Spread (%)': "{:.2f}%"})
    
    styled_df = display_df.drop(columns=cols_to_hide, errors='ignore').style.format(fmt)
    price_style_cols = [c for c in date_cols + ["Cotação atual"] if c in display_df.columns]
    styled_df.apply(lambda row: highlight_colunas_comparadas(row, price_style_cols), axis=1, subset=price_style_cols)
    styled_df.applymap(highlight_insights, subset=['Insight'])
    
    st.dataframe(styled_df, use_container_width=True)

# --- Funções para a Aba de Índices ---
@st.cache_data(ttl=3600)
def get_indices_historical_data(tickers: list[str]):
    try:
        hist_data = yf.download(tickers, start=date.today() - timedelta(days=370), end=date.today(), interval="1wk")
        return hist_data['Close'].dropna(how='all') if not hist_data.empty else pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao buscar dados históricos dos índices: {e}")
        return pd.DataFrame()

def normalize_data_for_charting(df: pd.DataFrame):
    return (df / df.iloc[0]) * 100 if not df.empty else df

def display_indices_tab():
    st.header("📈 Cotações de Índices e Commodities Relevantes")
    ticker_map = {
        "Ibovespa (B3)": "^BVSP", "IFIX (Índice de FIIs)": "IFIX.SA", "S&P 500 (EUA)": "^GSPC",
        "Nasdaq (EUA)": "^IXIC", "Dow Jones (EUA)": "^DJI", "Euro Stoxx 50 (UE)": "^STOXX50E",
        "Nikkei 225 (Japão)": "^N225", "VIX (S&P 500 Volatility)": "^VIX", "Juros EUA (Proxy 3M T-Bill)": "^IRX",
        "Ouro (USD)": "GC=F", "Prata (USD)": "SI=F", "Petróleo WTI (USD)": "CL=F",
        "Gás Natural (USD)": "NG=F", "Cobre (USD)": "HG=F", "Café (USD)": "KC=F",
        "Trigo (USD)": "ZW=F", "Soja (USD)": "ZS=F", "Dólar Comercial (USD/BRL)": "BRL=X",
        "Dívida Pública (Rendimento 10a - Brasil)": "BR10YT=X"
    }
    
    index_data = [get_index_data(ticker) for ticker in ticker_map.values()]
    df_indices = pd.DataFrame({"Ativo/Índice": list(ticker_map.keys()), "Cotação Atual": [d[0] for d in index_data], "Variação (%)": [d[1] for d in index_data]}).sort_values(by="Ativo/Índice").reset_index(drop=True)
    
    styled_df = df_indices.style.format({"Cotação Atual": "{:.2f}", "Variação (%)": "{:.2f}%"}).applymap(lambda v: 'color: green; font-weight: bold' if v>=0 else 'color: red; font-weight: bold', subset=['Variação (%)'])
    st.dataframe(styled_df, hide_index=True, use_container_width=True)

    st.markdown("---")
    st.header("📊 Análise Histórica Semanal (Últimos 12 Meses)")
    if st.checkbox("Exibir gráfico de desempenho histórico", key="show_indices_hist"):
        with st.spinner("Buscando dados históricos..."):
            hist_data = get_indices_historical_data(list(ticker_map.values()))
        if not hist_data.empty:
            hist_data.rename(columns={v: k for k, v in ticker_map.items()}, inplace=True)
            available = sorted([c for c in hist_data.columns if not hist_data[c].dropna().empty])
            default = [idx for idx in ["Ibovespa (B3)", "IFIX (Índice de FIIs)", "S&P 500 (EUA)", "Dólar Comercial (USD/BRL)"] if idx in available]
            selected = st.multiselect("Selecione para comparar:", options=available, default=default)
            if selected:
                norm_data = normalize_data_for_charting(hist_data[selected].dropna())
                st.info("Desempenho relativo normalizado para uma base 100 no início do período.")
                st.line_chart(norm_data)

# --- Função Principal da Aplicação ---
def main():
    st.sidebar.subheader("Pesos para o Score de Atratividade")
    # --- PESOS CORRIGIDOS ---
    weights = {
        'volatility': st.sidebar.slider("Peso Volatilidade", 0.0, 2.0, 1.0, 0.1, key="w_vol"),
        'moving_averages': st.sidebar.slider("Peso Médias Móveis", 0.0, 2.0, 1.0, 0.1, key="w_ma"),
        'rsi': st.sidebar.slider("Peso RSI", 0.0, 2.0, 1.0, 0.1, key="w_rsi"),
        'bollinger_bands': st.sidebar.slider("Peso Bandas de Bollinger", 0.0, 2.0, 1.0, 0.1, key="w_bb"),
        'momentum': st.sidebar.slider("Peso Momentum", 0.0, 2.0, 1.0, 0.1, key="w_mom"),
        'acceleration': st.sidebar.slider("Peso Aceleração", 0.0, 2.0, 1.0, 0.1, key="w_accel"),
        'growth_consistency': st.sidebar.slider("Peso Crescimento (Nível)", 0.0, 2.0, 1.0, 0.1, key="w_growth"),
    }
    
    tabs = st.tabs(["Ações (B3)", "Criptomoedas", "ETFs", "FIIs", "BDRs", "Índices"])
    sheet_map = {
        "Ações (B3)": (SHEET_NAME_STOCKS, "Ações"),
        "Criptomoedas": (SHEET_NAME_CRYPTO, "Cripto"),
        "ETFs": (SHEET_NAME_ETFS, "ETFs"),
        "FIIs": (SHEET_NAME_FIIS, "FIIs"),
        "BDRs": (SHEET_NAME_BDRS, "BDRs")
    }
    
    for i, tab_title in enumerate(sheet_map.keys()):
        with tabs[i]:
            st.header(f"Análise de {sheet_map[tab_title][1]} (Aba '{sheet_map[tab_title][0]}')")
            process_and_display_data(sheet_map[tab_title][0], sheet_map[tab_title][1], weights)

    with tabs[5]:
        display_indices_tab()

if __name__ == "__main__":
    main()