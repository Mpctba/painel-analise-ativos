# ÍNDICE
#
# 1. CONFIGURAÇÕES GLOBAIS E INICIALIZAÇÃO
#    1.1. Importação de Bibliotecas
#    1.2. Configurações da Página Streamlit
#    1.3. Definição de Constantes e Caminhos de Arquivos
#
# 2. FUNÇÕES DE AQUISIÇÃO E CARREGAMENTO DE DADOS
#    2.1. Carregamento de Dados Locais
#       2.1.1. carregar_planilha
#       2.1.2. load_default_tickers
#    2.2. Aquisição de Dados Online (yfinance)
#       2.2.1. get_price_var_min_max_last
#       2.2.2. get_index_data
#       2.2.3. get_next_ex_dividend_date
#       2.2.4. get_last_ex_dividend_date
#
# 3. FUNÇÕES DE CÁLCULO E ANÁLISE DE INDICADORES
#    3.1. Indicadores Baseados em Suporte e Resistência (SR)
#       3.1.1. calcular_sr
#       3.1.2. encontrar_valores_proximos
#       3.1.3. calcular_distancia_percentual
#       3.1.4. encontrar_var_faixa
#       3.1.5. calculate_historical_spread
#    3.2. Indicadores de Previsão e Tendência
#       3.2.1. prever_alvo
#       3.2.2. calculate_consecutive_growth
#    3.3. Análise de Ciclos Temporais
#       3.3.1. calculate_fft_period
#       3.3.2. calculate_acf_period
#       3.3.3. calculate_sin_fit_period
#       3.3.4. get_dominant_cycle
#       3.3.5. predict_target_from_cycle
#    3.4. Indicadores Técnicos Adicionais
#       3.4.1. calculate_additional_indicators
#       3.4.2. get_max_price_last_12_months
#       3.4.3. calculate_days_since_target_hit
#    3.5. Indicadores Relacionados a Dividendos
#       3.5.1. calculate_dividend_yield
#    3.6. Geração de Score e Insights
#       3.6.1. calculate_attractiveness_score
#       3.6.2. generate_insight
#
# 4. FUNÇÕES DE VISUALIZAÇÃO E ESTILIZAÇÃO
#    4.1. Estilização de DataFrames
#       4.1.1. highlight_colunas_comparadas
#       4.1.2. highlight_insights
#    4.2. Gráficos Interativos
#       4.2.1. visualize_price_data
#
# 5. LÓGICA PRINCIPAL DA APLICAÇÃO STREAMLIT
#    5.1. Processamento e Exibição por Tipo de Ativo
#       5.1.1. process_and_display_data
#    5.2. Exibição da Aba de Índices
#       5.2.1. display_indices_tab
#    5.3. Função Principal de Execução
#       5.3.1. main
#
# 6. PONTO DE ENTRADA DA EXECUÇÃO DO SCRIPT

# 1. CONFIGURAÇÕES GLOBAIS E INICIALIZAÇÃO
# 1.1. Importação de Bibliotecas
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

# 1.2. Configurações da Página Streamlit
st.set_page_config(page_title="📈 Análise de Preços Semanais - BOV2025", layout="wide")
st.title("📈 Análise de Preços Semanais - BOV2025")

# 1.3. Definição de Constantes e Caminhos de Arquivos
EXCEL_PATH = "BOV2025_Analise_Completa_B.xlsx"
SHEET_NAME_STOCKS = "Streamlit" # Aba para ações da B3
SHEET_NAME_CRYPTO = "Criptos" # Aba para criptomoedas
SHEET_NAME_ETFS = "ETF" # Aba para ETFs
SHEET_NAME_FIIS = "FII" # Nova aba para FIIs
SHEET_NAME_BDRS = "BDR" # Nova aba para BDRs (conforme solicitado, nome da aba no Excel)

HIDDEN_FILES = ["hidden_cols.txt", "hidden_col.txt"]
DEFAULT_TICKERS_FILE = "default_tickers.txt" # Novo arquivo para tickers padrão

# 2. FUNÇÕES DE AQUISIÇÃO E CARREGAMENTO DE DADOS
# 2.1. Carregamento de Dados Locais
# 2.1.1. carregar_planilha
@st.cache_data(ttl=300)
def carregar_planilha(path: str, aba: str) -> pd.DataFrame:
    """
    Carrega os dados de uma planilha Excel e normaliza os nomes das colunas.
    Remove colunas vazias e não nomeadas.
    """
    df = pd.read_excel(path, sheet_name=aba)
    df.columns = [unicodedata.normalize('NFC', str(col).strip()) for col in df.columns]
    df = df.dropna(axis=1, how="all")
    df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
    return df

# 2.2. Aquisição de Dados Online (yfinance)
# 2.2.1. get_price_var_min_max_last
@st.cache_data(ttl=3600)
def get_price_var_min_max_last(ticker_yf: str):
    """
    Busca dados de cotação usando yfinance, calcula variação,
    mínima/máxima de sextas-feiras e fechamento mais recente.
    Ajustado para lidar com fuso horário de São Paulo.
    Também retorna o histórico bruto para depuração.
    """
    try:
        ticker_data = yf.Ticker(ticker_yf)
        tz_sp = pytz.timezone('America/Sao_Paulo') # Fuso horário para o mercado brasileiro
        now_sp = dt.now(tz_sp)
        end_date_yf = now_sp.date() + timedelta(days=1) # Garante que o dia atual seja incluído

        hist = ticker_data.history(start="2024-06-01", end=end_date_yf)
        # Filtra explicitamente para garantir que os dados comecem em 2024-06-01
        hist = hist[hist.index.date >= date(2024, 6, 1)]

        if hist.empty:
            return None, None, None, None, None, pd.DataFrame()

        # Calcula a cotação atual e a variação diária
        recent = hist.tail(2)
        if len(recent) >= 2:
            close_today = round(recent["Close"].iloc[-1], 2)
            close_yesterday = recent["Close"].iloc[-2]
            var = round(((close_today - close_yesterday) / close_yesterday) * 100, 2)
        elif len(recent) == 1:
            close_today = round(recent["Close"].iloc[-1], 2)
            var = None
        else:
            close_today = None
            var = None

        # Filtra os dados apenas para sextas-feiras (dia da semana 4)
        sextas = hist[hist.index.weekday == 4]
        min_sexta = round(sextas["Close"].min(), 2) if not sextas.empty else None
        max_sexta = round(sextas["Close"].max(), 2) if not sextas.empty else None
        fechamento_mais_recente = round(sextas["Close"].iloc[-1], 2) if not sextas.empty else None

        return close_today, var, min_sexta, max_sexta, fechamento_mais_recente, hist
    except Exception as e:
        print(f"Erro ao buscar dados para {ticker_yf}: {e}")
        return None, None, None, None, None, pd.DataFrame()

# 2.2.2. get_index_data
@st.cache_data(ttl=600) # Cache por 10 minutos
def get_index_data(ticker_yf: str):
    """
    Busca dados de preço atual e variação diária para um ticker de índice.
    """
    try:
        ticker_data = yf.Ticker(ticker_yf)
        hist = ticker_data.history(period="2d") # Pega os últimos 2 dias para calcular a variação
        if hist.empty:
            return None, None

        close_today = round(hist["Close"].iloc[-1], 2)
        if len(hist) >= 2:
            close_yesterday = hist["Close"].iloc[-2]
            if close_yesterday != 0:
                var = round(((close_today - close_yesterday) / close_yesterday) * 100, 2)
            else:
                var = None
        else:
            var = None
        return close_today, var
    except Exception as e:
        print(f"Erro ao buscar dados para o índice {ticker_yf}: {e}")
        return None, None


# 3. FUNÇÕES DE CÁLCULO E ANÁLISE DE INDICADORES
# 3.1. Indicadores Baseados em Suporte e Resistência (SR)
# 3.1.1. calcular_sr
def calcular_sr(row):
    """Calcula os pontos de suporte e resistência (SR)."""
    H = row.get("Máxima sexta desde jun/24")
    L = row.get("Mínima sexta desde jun/24")
    C = row.get("Fechamento mais recente")
    if pd.notnull(H) and pd.notnull(L) and pd.notnull(C):
        P = (H + L + C) / 3
        return pd.Series([round(L - 2*(H-P), 2), round(P-(H-L), 2), round(2*P-H, 2), round(P, 2), round(2*P-L, 2), round(P+(H-L), 2), round(H+2*(P-L), 2)])
    return pd.Series([None]*7)

# 3.1.2. encontrar_valores_proximos
def encontrar_valores_proximos(row):
    """Encontra os níveis de suporte/resistência mais próximos da cotação atual."""
    preco = row.get("Cotação atual")
    niveis = [v for k, v in row.items() if k in ["S3","S2","S1","P","R1","R2","R3"] and pd.notnull(v)]
    niveis.sort()
    abaixo = max([v for v in niveis if v<=preco], default=None)
    acima = min([v for v in niveis if v>preco], default=None)
    return pd.Series([abaixo,acima])

# 3.1.3. calcular_distancia_percentual
def calcular_distancia_percentual(row):
    """Calcula a menor distância percentual da cotação atual para o nível mais próximo."""
    preco = row.get("Cotação atual")
    abaixo = row.get("Nível abaixo")
    acima = row.get("Nível acima")
    d1 = abs((preco-abaixo)/preco)*100 if pd.notnull(abaixo) and preco != 0 else None
    d2 = abs((acima-preco)/preco)*100 if pd.notnull(acima) and preco != 0 else None
    return round(min([d for d in [d1,d2] if d is not None], default=None), 2) if d1 is not None or d2 is not None else None

# 3.1.4. encontrar_var_faixa
def encontrar_var_faixa(row, k_values_list):
    """Encontra a faixa de variação (K) em que a variação atual se encaixa."""
    var = row.get("Var")
    arr = sorted([v for v in k_values_list if pd.notnull(v)])
    if pd.notnull(var) and arr:
        aba = max([v for v in arr if v<=var], default=None)
        ac = min([v for v in arr if v > var], default=None)
        return pd.Series([aba, ac])
    return pd.Series([None, None])

# 3.2. Indicadores de Previsão e Tendência
# 3.2.1. prever_alvo
def prever_alvo(row, last_cols, last_dates, next_friday):
    """Prevê o valor alvo usando regressão linear simples."""
    ys = [row[c] for c in last_cols]
    valid_indices = [i for i, y in enumerate(ys) if pd.notnull(y)]
    if len(valid_indices) < 2:
        return None

    valid_ys = [ys[i] for i in valid_indices]
    valid_xs = [last_dates[i].toordinal() for i in valid_indices]

    if len(valid_xs) < 2:
        return None

    m, b = np.polyfit(valid_xs, valid_ys, 1)
    return round(m * next_friday.toordinal() + b, 2)

# 4. FUNÇÕES DE VISUALIZAÇÃO E ESTILIZAÇÃO
# 4.1. Estilização de DataFrames
# 4.1.1. highlight_colunas_comparadas
def highlight_colunas_comparadas(row, colunas_para_estilo):
    """
    Aplica estilo de cor (verde/vermelho) às colunas de cotação
    com base na comparação com o valor anterior.
    """
    vals = row[colunas_para_estilo].values
    styles = [''] * len(vals)
    for i in range(1, len(vals)):
        ant = vals[i-1]
        atual = vals[i]

        if pd.notnull(ant) and pd.notnull(atual):
            if atual >= ant:
                styles[i] = 'color: green; font-weight: bold'
            elif atual < ant:
                styles[i] = 'color: red; font-weight: bold'
    return styles

# 4.1.2. highlight_insights
def highlight_insights(val):
    """Aplica cor à célula de Insight com base no seu conteúdo."""
    if isinstance(val, str):
        if "Compra Forte" in val or "Atenção para Compra" in val:
            return 'color: green; font-weight: bold'
        elif "Evitar / Atenção" in val:
            return 'color: red; font-weight: bold'
    return ''

# 3.2.2. calculate_consecutive_growth
def calculate_consecutive_growth(row, static_date_cols, current_quote_col):
    """
    Calcula o número de semanas consecutivas de crescimento.
    """
    all_relevant_values = []
    for col in static_date_cols:
        val = row.get(col)
        all_relevant_values.append(val if pd.notnull(val) else np.nan)

    current_quote = row.get(current_quote_col)
    values_for_streak = []

    if len(all_relevant_values) > 0 and pd.notnull(current_quote):
        last_static_value = all_relevant_values[-1]
        if pd.notnull(last_static_value) and current_quote > last_static_value:
            values_for_streak = all_relevant_values + [current_quote]
        else:
            values_for_streak = all_relevant_values
    else:
        values_for_streak = all_relevant_values

    if len(values_for_streak) < 2:
        return 0

    consecutive_growth_count = 0
    for i in range(len(values_for_streak) - 1, 0, -1):
        curr_val = values_for_streak[i]
        prev_val = values_for_streak[i-1]

        if pd.isna(curr_val) or pd.isna(prev_val):
            break
        elif curr_val > prev_val:
            consecutive_growth_count += 1
        else:
            break
    return consecutive_growth_count

# 2.1.2. load_default_tickers
def load_default_tickers(file_path: str, all_options: list) -> list:
    """
    Carrega tickers padrão de um arquivo de texto.
    """
    default_tickers = []
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                loaded_tickers = [unicodedata.normalize('NFC', line.strip()) for line in f if line.strip()]
                default_tickers = [t for t in loaded_tickers if t in all_options]
        except Exception as e:
            st.warning(f"Erro ao carregar tickers padrão do arquivo '{file_path}': {e}")
    return default_tickers

# 3.1.5. calculate_historical_spread
def calculate_historical_spread(hist_data: pd.DataFrame):
    """
    Calcula o 'Spread (%)' para cada dia no histórico de dados.
    """
    if hist_data.empty:
        return pd.Series(dtype=float)

    historical_spreads = []
    hist_data.index = pd.to_datetime(hist_data.index)
    sr_keys = ["S3","S2","S1","P","R1","R2","R3"]
    nearest_level_keys = ["Nível abaixo", "Nível acima"]
    var_faixa_keys = ["Var (abaixo)", "Var (acima)"]

    for i in range(len(hist_data)):
        current_date = hist_data.index[i]
        daily_hist = hist_data.loc[hist_data.index <= current_date]
        sextas_up_to_date = daily_hist[daily_hist.index.weekday == 4]

        H_D = round(sextas_up_to_date["Close"].max(), 2) if not sextas_up_to_date.empty else None
        L_D = round(sextas_up_to_date["Close"].min(), 2) if not sextas_up_to_date.empty else None
        C_D = round(sextas_up_to_date["Close"].iloc[-1], 2) if not sextas_up_to_date.empty else None
        Preco_D = round(daily_hist["Close"].iloc[-1], 2)
        Var_D = None
        if len(daily_hist) >= 2:
            prev_close = daily_hist["Close"].iloc[-2]
            if prev_close != 0:
                Var_D = round(((Preco_D - prev_close) / prev_close) * 100, 2)

        temp_row_series = pd.Series({
            "Máxima sexta desde jun/24": H_D, "Mínima sexta desde jun/24": L_D,
            "Fechamento mais recente": C_D, "Cotação atual": Preco_D, "Var": Var_D
        })

        sr_points = calcular_sr(temp_row_series)
        for j, key in enumerate(sr_keys):
            if j < len(sr_points): temp_row_series[key] = sr_points.iloc[j]

        nearest_levels = encontrar_valores_proximos(temp_row_series)
        for j, key in enumerate(nearest_level_keys):
            if j < len(nearest_levels): temp_row_series[key] = nearest_levels.iloc[j]

        temp_row_series["Delta"] = calcular_distancia_percentual(temp_row_series)
        amplitude = None
        if pd.notnull(temp_row_series.get("Nível abaixo")) and temp_row_series.get("Nível abaixo") != 0 and pd.notnull(temp_row_series.get("Nível acima")):
            amplitude = round(((temp_row_series.get("Nível acima")/temp_row_series.get("Nível abaixo")-1)*100), 2)
        temp_row_series["Amplitude"] = amplitude

        k_div = [-2,-3,-5,-9,-17,-33,-65,65,33,17,9,5,3,2]
        k_values = [round(amplitude/k, 2) if pd.notnull(amplitude) else None for k in k_div]
        var_faixa = encontrar_var_faixa(temp_row_series, k_values)
        for j, key in enumerate(var_faixa_keys):
            if j < len(var_faixa): temp_row_series[key] = var_faixa.iloc[j]

        spread = None
        if pd.notnull(temp_row_series.get("Var (abaixo)")) and pd.notnull(temp_row_series.get("Var (acima)")):
            spread = round(temp_row_series.get("Var (acima)") - temp_row_series.get("Var (abaixo)"), 2)
        historical_spreads.append(spread)

    return pd.Series(historical_spreads, index=hist_data.index, name="Historical_Spread_Pct")

# 3.3. Análise de Ciclos Temporais
# 3.3.1. calculate_fft_period
def calculate_fft_period(series: pd.Series, use_log_returns: bool = True) -> float | None:
    """
    Calcula o período dominante usando a Transformada Rápida de Fourier (FFT).
    """
    if series.empty or len(series.dropna()) < 2: return None
    data_to_analyze = np.log(series / series.shift(1)).dropna().values if use_log_returns else series.dropna().values
    if len(data_to_analyze) < 2: return None

    N = len(data_to_analyze)
    yf = fft(data_to_analyze)
    xf = fftfreq(N, 1)

    amplitudes = 2.0/N * np.abs(yf[0:N//2])
    frequencias = xf[0:N//2]

    if np.all(amplitudes[1:] == 0) or len(amplitudes[1:]) == 0: return None

    idx_dominante = np.argmax(amplitudes[1:]) + 1
    frequencia_dominante = frequencias[idx_dominante]

    if frequencia_dominante == 0: return None
    return round(1 / frequencia_dominante, 2)

# 3.3.2. calculate_acf_period
def calculate_acf_period(series: pd.Series, use_log_returns: bool = True) -> int | None:
    """
    Calcula o período usando a Função de Autocorrelação (ACF).
    """
    if series.empty or len(series.dropna()) < 2: return None
    data_to_analyze = np.log(series / series.shift(1)).dropna().values if use_log_returns else series.dropna().values
    if len(data_to_analyze) < 2: return None

    nlags = min(50, len(data_to_analyze) - 1)
    if nlags < 1: return None

    autocorr_values = acf(data_to_analyze, nlags=nlags, fft=True)

    if np.all(autocorr_values[1:] == 0) or len(autocorr_values[1:]) == 0: return None
    return np.argmax(np.abs(autocorr_values[1:])) + 1

# 3.3.3. calculate_sin_fit_period
def calculate_sin_fit_period(series: pd.Series) -> float | None:
    """
    Tenta ajustar uma função senoidal aos dados e retorna o período.
    """
    data = series.dropna()
    if data.empty or len(data) < 10: return None

    x_data = np.arange(len(data))
    y_data = data.values

    def sinusoidal(t, amplitude, periodo, fase, offset):
        return amplitude * np.sin(2 * np.pi * t / periodo + fase) + offset

    p0 = [np.std(y_data), 20, 0, np.mean(y_data)]
    bounds = ([0, 1, -np.inf, -np.inf], [np.inf, min(252, len(data)), np.inf, np.inf])

    try:
        params, _ = curve_fit(sinusoidal, x_data, y_data, p0=p0, bounds=bounds, maxfev=5000)
        periodo_fit = params[1]
        return round(periodo_fit, 2) if periodo_fit > 0 else None
    except (RuntimeError, ValueError):
        return None

# 3.3.4. get_dominant_cycle
def get_dominant_cycle(fft_p, acf_p, sin_p) -> tuple[float | None, str]:
    """
    Avalia a consistência entre os períodos de ciclo e retorna o período dominante.
    """
    periods = [p for p in [fft_p, acf_p, sin_p] if pd.notnull(p)]
    if not periods: return None, "Nenhum ciclo detectado."
    if len(periods) == 1: return periods[0], f"Apenas um método (Período: {periods[0]:.2f}d)."

    consistent_periods = []
    if fft_p is not None and acf_p is not None and abs(fft_p - acf_p) / max(fft_p, acf_p) < 0.2:
        consistent_periods.extend([fft_p, acf_p])
    if fft_p is not None and sin_p is not None and abs(fft_p - sin_p) / max(fft_p, sin_p) < 0.2:
        consistent_periods.extend([fft_p, sin_p])
    if acf_p is not None and sin_p is not None and abs(acf_p - sin_p) / max(acf_p, sin_p) < 0.2:
        consistent_periods.extend([acf_p, sin_p])

    if consistent_periods:
        dominant_period = round(np.mean(list(set(consistent_periods))), 2)
        return dominant_period, f"Consistente: **{dominant_period}d** (~{round(dominant_period / 7, 1)}sem)."
    else:
        return None, "Inconsistente. Ruído nos dados."

# 3.3.5. predict_target_from_cycle
def predict_target_from_cycle(hist_data: pd.DataFrame, dominant_cycle_days: float) -> float | None:
    """
    Prevê um alvo futuro baseado no período de ciclo dominante.
    """
    if hist_data.empty or dominant_cycle_days is None or dominant_cycle_days <= 1 or len(hist_data) < 10: return None
    data = hist_data['Close'].dropna()
    if data.empty or len(data) < 10: return None

    x_data = np.arange(len(data))
    y_data = data.values

    def sinusoidal(t, amplitude, fase, offset):
        return amplitude * np.sin(2 * np.pi * t / dominant_cycle_days + fase) + offset

    p0 = [np.std(y_data), 0, np.mean(y_data)]
    try:
        params, _ = curve_fit(sinusoidal, x_data, y_data, p0=p0, bounds=([0, -np.inf, -np.inf], [np.inf, np.inf, np.inf]), maxfev=5000)
        future_x = len(data) + (dominant_cycle_days / 4)
        return round(sinusoidal(future_x, *params), 2)
    except Exception:
        return None

# 3.4. Indicadores Técnicos Adicionais
# 3.4.1. calculate_additional_indicators
def calculate_additional_indicators(hist_data: pd.DataFrame) -> pd.Series:
    """
    Calcula vários indicadores técnicos para o histórico de dados fornecido.
    """
    if hist_data.empty or len(hist_data) < 20:
        return pd.Series({k: None for k in ['Volatilidade_Anualizada', 'SMA_20_Dias', 'EMA_20_Dias', 'BB_Upper', 'BB_Lower', 'RSI_14_Dias', 'Volume_Medio_20_Dias', 'Momentum_10_Dias', 'Aceleracao_10_Dias']})

    hist_data = hist_data.copy().sort_index()
    hist_data.index = pd.to_datetime(hist_data.index)

    log_returns = np.log(hist_data['Close'] / hist_data['Close'].shift(1))
    vol = log_returns.rolling(window=20).std().iloc[-1] * np.sqrt(252) * 100
    sma_20 = hist_data['Close'].rolling(window=20).mean().iloc[-1]
    ema_20 = hist_data['Close'].ewm(span=20, adjust=False).mean().iloc[-1]
    std_dev_20 = hist_data['Close'].rolling(window=20).std().iloc[-1]
    bb_upper = sma_20 + (std_dev_20 * 2)
    bb_lower = sma_20 - (std_dev_20 * 2)

    delta = hist_data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss if loss.iloc[-1] != 0 else pd.Series([np.inf])
    rsi_14 = (100 - (100 / (1 + rs))).iloc[-1]

    vol_med_20 = hist_data['Volume'].rolling(window=20).mean().iloc[-1]
    
    momentum_10 = None
    if len(hist_data) > 10:
        momentum_10 = ((hist_data['Close'].iloc[-1] - hist_data['Close'].iloc[-11]) / hist_data['Close'].iloc[-11]) * 100

    aceleracao_10 = None
    if len(hist_data) > 10:
        aceleracao_10 = (hist_data['Close'].pct_change() * 100).iloc[-10:].mean()

    return pd.Series({
        'Volatilidade_Anualizada': round(vol, 2) if pd.notnull(vol) else None,
        'SMA_20_Dias': round(sma_20, 2) if pd.notnull(sma_20) else None,
        'EMA_20_Dias': round(ema_20, 2) if pd.notnull(ema_20) else None,
        'BB_Upper': round(bb_upper, 2) if pd.notnull(bb_upper) else None,
        'BB_Lower': round(bb_lower, 2) if pd.notnull(bb_lower) else None,
        'RSI_14_Dias': round(rsi_14, 2) if pd.notnull(rsi_14) else None,
        'Volume_Medio_20_Dias': round(vol_med_20, 0) if pd.notnull(vol_med_20) else None,
        'Momentum_10_Dias': round(momentum_10, 2) if pd.notnull(momentum_10) else None,
        'Aceleracao_10_Dias': round(aceleracao_10, 2) if pd.notnull(aceleracao_10) else None
    })

# 3.6. Geração de Score e Insights
# 3.6.1. calculate_attractiveness_score
def calculate_attractiveness_score(row: pd.Series, weights: dict) -> float:
    """
    Calcula uma pontuação de atratividade de 0 a 10 para um ticker.
    """
    score = 0.0
    
    # Mapeamento de critérios e suas contribuições
    criteria = {
        'volatility': (row.get('Volatilidade_Anualizada'), [
            (lambda v: v < 20, 1.5), (lambda v: 20 <= v < 40, 0.5)
        ]),
        'moving_averages': [
            (row.get('Cotação atual') > row.get('SMA_20_Dias'), 1.5),
            (row.get('Cotação atual') > row.get('EMA_20_Dias'), 1.5)
        ],
        'rsi': (row.get('RSI_14_Dias'), [
            (lambda r: r < 30, 2.5), (lambda r: 30 <= r <= 70, 0.5)
        ]),
        'bollinger_bands': (row.get('Cotação atual'), [
            (lambda p: p < row.get('BB_Lower', np.inf) * 1.01, 2.0),
            (lambda p: row.get('BB_Lower', np.inf) < p < row.get('BB_Upper', -np.inf), 0.5)
        ]),
        'volume': (row.get('Volume_Medio_20_Dias') > 0, 0.5),
        'cycles': (get_dominant_cycle(row.get('Ciclo_FFT_Dias'), row.get('Ciclo_ACF_Dias'), row.get('Ciclo_Sinoidal_Dias'))[0] is not None, 0.5),
        'spread_bonus': (row.get('Spread (%)', 0) > 1.0 and row.get('Var', 0) < 0, 1.0),
        'target_proximity': (row.get('Alvo'), [
            (lambda a: 0 < ((a - row.get('Cotação atual', 0)) / row.get('Cotação atual', 1)) * 100 <= 5, 1.0),
            (lambda a: 5 < ((a - row.get('Cotação atual', 0)) / row.get('Cotação atual', 1)) * 100 <= 15, 0.5)
        ]),
        'growth_consistency': (row.get('Nível', 0), [
            (lambda n: n >= 3, 1.5), (lambda n: n == 2, 0.5)
        ]),
        'momentum': (row.get('Momentum_10_Dias', 0), [
            (lambda m: m > 5, 1.0), (lambda m: m > 0, 0.5)
        ]),
        'acceleration': (row.get('Aceleracao_10_Dias', 0), [
            (lambda a: a > 1.0, 1.0), (lambda a: a > 0, 0.5)
        ])
    }

    for key, data in criteria.items():
        weight = weights.get(key, 1.0)
        if isinstance(data, tuple):
            value, conditions = data
            if pd.notnull(value):
                if isinstance(conditions, list):
                    for cond, points in conditions:
                        if cond(value):
                            score += points * weight
                            break
                elif conditions:
                    score += 0.5 * weight
        elif isinstance(data, list):
            for cond, points in data:
                if pd.notnull(row.get('Cotação atual')) and all(pd.notnull(row.get(s.split("'")[1])) for s in str(cond).split() if "row.get" in s) and cond:
                     score += points * weight

    return round(min(10.0, score), 2)

# 3.6.2. generate_insight
def generate_insight(row: pd.Series) -> str:
    """
    Gera uma string de insight com base na pontuação de atratividade.
    """
    score = row.get("Score")
    if pd.notnull(score):
        if score >= 8.0: return "Compra Forte"
        if score >= 6.0: return "Atenção para Compra"
        if score >= 4.0: return "Monitorar"
        return "Evitar / Atenção"
    return "Sem dados para análise"


# 3.4.2. get_max_price_last_12_months
def get_max_price_last_12_months(hist_data: pd.DataFrame) -> float | None:
    """
    Calcula o preço máximo (High) do ativo nos últimos 12 meses.
    """
    if hist_data.empty or 'High' not in hist_data.columns: return None
    most_recent_date = hist_data.index.max()
    start_date = most_recent_date - timedelta(days=365)
    recent_hist = hist_data.loc[hist_data.index >= start_date]
    if recent_hist.empty: return None
    max_price = recent_hist['High'].max()
    return round(max_price, 2) if pd.notnull(max_price) else None

# 3.4.3. calculate_days_since_target_hit
def calculate_days_since_target_hit(hist_data: pd.DataFrame, target_price: float) -> int | None:
    """
    Calcula dias desde que o alvo de preço foi atingido.
    """
    if hist_data is None or hist_data.empty or 'Close' not in hist_data.columns or pd.isna(target_price): return None
    hist_data = hist_data.sort_index()
    dates_hit_target = hist_data[hist_data['Close'] >= target_price].index
    if dates_hit_target.empty: return None
    return (hist_data.index.max() - dates_hit_target.max()).days

# 3.5. Indicadores Relacionados a Dividendos
# 3.5.1. calculate_dividend_yield
def calculate_dividend_yield(hist_data: pd.DataFrame, current_price: float) -> float | None:
    """
    Calcula o Dividend Yield (DY) anualizado.
    """
    if hist_data.empty or 'Dividends' not in hist_data.columns or pd.isna(current_price) or current_price == 0: return None
    most_recent_date = hist_data.index.max()
    start_date = most_recent_date - timedelta(days=365)
    recent_dividends = hist_data.loc[hist_data.index >= start_date, 'Dividends']
    total_dividends = recent_dividends.sum()
    if total_dividends == 0: return 0.0
    return round((total_dividends / current_price) * 100, 2)

# 2.2.3. get_next_ex_dividend_date
@st.cache_data(ttl=3600)
def get_next_ex_dividend_date(ticker_yf: str) -> date | None:
    """
    Busca a próxima data ex-dividendo.
    """
    try:
        dividends = yf.Ticker(ticker_yf).actions[lambda x: x['Dividends'] > 0]
        if dividends.empty: return None
        future_dividends = dividends[dividends.index.tz_localize(None).date >= date.today()].sort_index()
        return future_dividends.index[0].date() if not future_dividends.empty else None
    except Exception: return None

# 2.2.4. get_last_ex_dividend_date
@st.cache_data(ttl=3600)
def get_last_ex_dividend_date(ticker_yf: str) -> date | None:
    """
    Busca a última data ex-dividendo (passada).
    """
    try:
        dividends = yf.Ticker(ticker_yf).actions[lambda x: x['Dividends'] > 0]
        if dividends.empty: return None
        past_dividends = dividends[dividends.index.tz_localize(None).date < date.today()].sort_index(ascending=False)
        return past_dividends.index[0].date() if not past_dividends.empty else None
    except Exception: return None

# 4.2. Gráficos Interativos
# 4.2.1. visualize_price_data
def visualize_price_data(hist_data: pd.DataFrame, ticker: str, sr_levels: dict, events_df: pd.DataFrame = None):
    """
    Cria um gráfico interativo do histórico de preços com indicadores.
    """
    if hist_data.empty:
        st.warning(f"Não há dados históricos para exibir o gráfico de {ticker}.")
        return

    hist_data = hist_data.copy()
    hist_data.index = pd.to_datetime(hist_data.index)
    
    if len(hist_data) >= 20:
        hist_data['SMA_20_Dias'] = hist_data['Close'].rolling(window=20).mean()
        hist_data['StdDev'] = hist_data['Close'].rolling(window=20).std()
        hist_data['BB_Upper'] = hist_data['SMA_20_Dias'] + (hist_data['StdDev'] * 2)
        hist_data['BB_Lower'] = hist_data['SMA_20_Dias'] - (hist_data['StdDev'] * 2)

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=[0.7, 0.3])
    fig.add_trace(go.Candlestick(x=hist_data.index, open=hist_data['Open'], high=hist_data['High'], low=hist_data['Low'], close=hist_data['Close'], name='Preço'), row=1, col=1)

    if 'SMA_20_Dias' in hist_data.columns:
        fig.add_trace(go.Scatter(x=hist_data.index, y=hist_data['SMA_20_Dias'], line=dict(color='orange', width=1), name='SMA 20'), row=1, col=1)
    if 'BB_Upper' in hist_data.columns:
        fig.add_trace(go.Scatter(x=hist_data.index, y=hist_data['BB_Upper'], line=dict(color='gray', width=1, dash='dash'), name='BB Superior'), row=1, col=1)
        fig.add_trace(go.Scatter(x=hist_data.index, y=hist_data['BB_Lower'], line=dict(color='gray', width=1, dash='dash'), name='BB Inferior'), row=1, col=1)

    colors = ['#FF4136', '#FF851B', '#FFDC00', '#2ECC40', '#0074D9', '#3D9970', '#85144B']
    for i, level_name in enumerate(['S3', 'S2', 'S1', 'P', 'R1', 'R2', 'R3']):
        if pd.notnull(sr_levels.get(level_name)):
            fig.add_hline(y=sr_levels[level_name], line_dash="dash", line_color=colors[i], annotation_text=level_name, annotation_position="top left", row=1, col=1)

    if events_df is not None and not events_df.empty:
        dividend_events = events_df[events_df['Dividends'] > 0]
        if not dividend_events.empty:
            dividend_dates = hist_data.index.intersection(dividend_events.index)
            if not dividend_dates.empty:
                fig.add_trace(go.Scatter(x=dividend_dates, y=hist_data.loc[dividend_dates, 'Close'], mode='markers', marker=dict(symbol='triangle-up', size=8, color='blue'), name='Dividendos', text=[f"Div: {d:.2f}" for d in dividend_events.loc[dividend_dates, 'Dividends']], hoverinfo='text+x+y'), row=1, col=1)
    
    if 'Volume' in hist_data.columns:
        fig.add_trace(go.Bar(x=hist_data.index, y=hist_data['Volume'], name='Volume', marker_color='lightblue'), row=2, col=1)

    fig.update_layout(title_text=f'Análise de Preços para {ticker}', xaxis_rangeslider_visible=False, height=600, hovermode='x unified')
    fig.update_yaxes(title_text='Preço', row=1, col=1)
    fig.update_yaxes(title_text='Volume', row=2, col=1)
    st.plotly_chart(fig, use_container_width=True)

# 5. LÓGICA PRINCIPAL DA APLICAÇÃO STREAMLIT
# 5.1. Processamento e Exibição por Tipo de Ativo
# 5.1.1. process_and_display_data
def process_and_display_data(sheet_name: str, asset_type_display_name: str, weights: dict):
    """
    Função principal que processa e exibe os dados para um tipo de ativo.
    """
    df = carregar_planilha(EXCEL_PATH, sheet_name)
    if df.empty:
        st.info(f"A planilha '{sheet_name}' está vazia.")
        return
    if "Ticker" not in df.columns:
        st.warning(f"Coluna 'Ticker' não encontrada na aba '{sheet_name}'.")
        return

    # Processamento de dados
    df["Ticker_YF"] = df["Ticker"].astype(str).str.strip() + (".SA" if asset_type_display_name in ["Ações", "ETFs", "FIIs", "BDRs"] else "")
    price_data = df["Ticker_YF"].apply(lambda t: pd.Series(get_price_var_min_max_last(t), index=["Cotação atual", "Var", "Mínima sexta desde jun/24", "Máxima sexta desde jun/24", "Fechamento mais recente", "Raw_Hist_Data"]))
    df = pd.concat([df, price_data], axis=1)

    df[["S3","S2","S1","P","R1","R2","R3"]] = df.apply(calcular_sr, axis=1)
    df[["Nível abaixo","Nível acima"]] = df.apply(encontrar_valores_proximos, axis=1)
    df["Delta"] = df.apply(calcular_distancia_percentual, axis=1)
    df["Amplitude"] = df.apply(lambda r: round(((r.get("Nível acima")/r.get("Nível abaixo")-1)*100), 2) if pd.notnull(r.get("Nível abaixo")) and r.get("Nível abaixo")!=0 else None, axis=1)
    k_divs = [-2,-3,-5,-9,-17,-33,-65,65,33,17,9,5,3,2]
    k_cols = [f"K ({k})" for k in k_divs]
    df[k_cols] = df["Amplitude"].apply(lambda amp: pd.Series([round(amp/k, 2) if pd.notnull(amp) else None for k in k_divs]))
    df[["Var (abaixo)","Var (acima)"]] = df.apply(lambda row: encontrar_var_faixa(row, [row[c] for c in k_cols]), axis=1)
    df["Spread (%)"] = df.apply(lambda r: round(r.get("Var (acima)")-r.get("Var (abaixo)"), 2) if pd.notnull(r.get("Var (abaixo)")) and pd.notnull(r.get("Var (acima)")) else None, axis=1)

    date_cols = sorted([c for c in df.columns if c[:4].isdigit() and "-" in c])
    for c in date_cols: df[c] = pd.to_numeric(df[c], errors="coerce")

    if len(date_cols) >= 4:
        last_cols = date_cols[-4:]
        last_dates = [pd.to_datetime(col).date() for col in last_cols]
        next_friday = date.today() + timedelta(days=(4 - date.today().weekday()) % 7)
        df['Alvo'] = df.apply(lambda row: prever_alvo(row, last_cols, last_dates, next_friday), axis=1)
    else:
        df['Alvo'] = None

    df['Maxima_12_Meses'] = df['Raw_Hist_Data'].apply(get_max_price_last_12_months)
    df['Dias_Alvo'] = df.apply(lambda r: "Máxima" if pd.notnull(r["Alvo"]) and pd.notnull(r["Maxima_12_Meses"]) and r["Alvo"] >= r["Maxima_12_Meses"] else calculate_days_since_target_hit(r["Raw_Hist_Data"], r["Alvo"]), axis=1)
    df['Nível'] = df.apply(lambda r: calculate_consecutive_growth(r, date_cols[-5:], "Cotação atual"), axis=1)

    # Filtros e exibição
    opt = df["Ticker"].unique().tolist()
    default_selected = load_default_tickers(DEFAULT_TICKERS_FILE, opt)
    sel = st.multiselect(f"Filtrar por Ticker ({asset_type_display_name}):", options=opt, default=default_selected, key=f"multiselect_{sheet_name}")
    df = df[df["Ticker"].isin(sel)] if sel else df

    if df.empty:
        st.info("Nenhum ticker selecionado.")
        return

    # Análises avançadas
    if st.checkbox(f"Realizar Análise de Ciclos", key=f"cycle_analysis_{sheet_name}"):
        df['Ciclo_FFT_Dias'] = df['Raw_Hist_Data'].apply(lambda x: calculate_fft_period(x["Close"]))
        df['Ciclo_ACF_Dias'] = df['Raw_Hist_Data'].apply(lambda x: calculate_acf_period(x["Close"]))
        df['Ciclo_Sinoidal_Dias'] = df['Raw_Hist_Data'].apply(lambda x: calculate_sin_fit_period(x["Close"]))
        df[['Ciclo_Dominante_Dias', 'Status_Ciclo']] = df.apply(lambda r: pd.Series(get_dominant_cycle(r['Ciclo_FFT_Dias'], r['Ciclo_ACF_Dias'], r['Ciclo_Sinoidal_Dias'])), axis=1)
        df['Alvo_Ciclo'] = df.apply(lambda r: predict_target_from_cycle(r['Raw_Hist_Data'], r['Ciclo_Dominante_Dias']), axis=1)

    indicator_data = df["Raw_Hist_Data"].apply(calculate_additional_indicators)
    df = pd.concat([df, indicator_data], axis=1)
    df['Score'] = df.apply(lambda row: calculate_attractiveness_score(row, weights), axis=1)
    df['Insight'] = df.apply(generate_insight, axis=1)
    df['DY (%)'] = df.apply(lambda r: calculate_dividend_yield(r["Raw_Hist_Data"], r["Cotação atual"]), axis=1)
    if asset_type_display_name in ["Ações", "FIIs", "BDRs"]:
        df['Data_ex'] = df['Ticker_YF'].apply(get_next_ex_dividend_date)
        df['Ultima_Data_ex'] = df['Ticker_YF'].apply(get_last_ex_dividend_date)

    # Exibição do DataFrame
    # ... (lógica de ordenação e formatação das colunas para exibição)
    st.dataframe(df) # Simplificado para o exemplo, a lógica de formatação e ordenação completa deve ser mantida

# 5.2. Exibição da Aba de Índices
# 5.2.1. display_indices_tab
def display_indices_tab():
    """
    Exibe a aba de índices com cotações e o gráfico comparativo de performance.
    """
    st.header("📈 Cotações de Índices e Commodities Relevantes")

    indices_disponiveis = {
        "Ibovespa (B3)": "^BVSP", "S&P 500 (EUA)": "^GSPC", "Nasdaq (EUA)": "^IXIC",
        "Dólar Comercial (USD/BRL)": "BRL=X", "Ouro (USD)": "GC=F", "Petróleo WTI (USD)": "CL=F"
        # Adicione outros aqui se desejar
    }
    
    # Tabela de Cotações (lógica simplificada para focar na mudança principal)
    # A sua lógica original de buscar cada um e montar o DF pode ser mantida
    st.info("A tabela de cotações diárias foi omitida para simplificar. Mantenha seu código original aqui.")

    st.write("---")

    # --- Seção Comparativo de Performance (COM A LÓGICA ROBUSTA) ---
    st.subheader("Comparativo de Performance Semanal (Últimos 12 Meses)")
    
    selecionados = st.multiselect(
        "Selecione os ativos para comparar:",
        options=list(indices_disponiveis.keys()),
        default=["Ibovespa (B3)", "S&P 500 (EUA)", "Dólar Comercial (USD/BRL)"]
    )

    if selecionados:
        end_date = date.today()
        start_date = end_date - timedelta(days=365)
        closes_list = []
        column_names = []

        with st.spinner("Buscando dados históricos..."):
            for nome_indice in selecionados:
                ticker_yf = indices_disponiveis[nome_indice]
                try:
                    hist = yf.Ticker(ticker_yf).history(start=start_date, end=end_date, auto_adjust=True)
                    if not hist.empty:
                        closes_list.append(hist['Close'])
                        column_names.append(nome_indice)
                    else:
                        st.warning(f"Não foram encontrados dados históricos para '{nome_indice}'.")
                except Exception as e:
                    st.error(f"Erro ao buscar dados para '{nome_indice}': {e}")
        
        if closes_list:
            all_closes = pd.concat(closes_list, axis=1)
            all_closes.columns = column_names
            
            with st.spinner("Calculando performance..."):
                weekly_closes = all_closes.resample('W-FRI').last()
                weekly_closes.dropna(how='all', inplace=True)

                # --- LÓGICA DE NORMALIZAÇÃO ROBUSTA ---
                performance_df = pd.DataFrame(index=weekly_closes.index)
                for col in weekly_closes.columns:
                    series = weekly_closes[col].dropna()
                    if not series.empty:
                        first_valid_value = series.iloc[0]
                        if pd.notnull(first_valid_value) and first_valid_value != 0:
                            performance_df[col] = (weekly_closes[col] / first_valid_value) * 100
                
                performance_df.dropna(axis=1, how='all', inplace=True)

            fig = go.Figure()
            for indice in performance_df.columns:
                fig.add_trace(go.Scatter(
                    x=performance_df.index,
                    y=performance_df[indice],
                    mode='lines',
                    name=indice,
                    connectgaps=True
                ))

            fig.update_layout(
                title="Performance Normalizada (Base 100)",
                yaxis_title="Performance (Primeiro dia = 100)",
                xaxis_title="Data",
                legend_title="Ativos",
                hovermode="x unified"
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Selecione um ou mais ativos para visualizar a comparação.")

# 5.3. Função Principal de Execução
# 5.3.1. main
def main():
    """
    Função principal que organiza a interface do Streamlit.
    """
    try:
        st.sidebar.subheader("Pesos para o Score de Atratividade")
        weights = {
            'volatility': st.sidebar.slider("Peso Volatilidade", 0.0, 2.0, 1.0, 0.1),
            'moving_averages': st.sidebar.slider("Peso Médias Móveis", 0.0, 2.0, 1.0, 0.1),
            'rsi': st.sidebar.slider("Peso RSI", 0.0, 2.0, 1.0, 0.1),
            'bollinger_bands': st.sidebar.slider("Peso Bandas de Bollinger", 0.0, 2.0, 1.0, 0.1),
            'volume': st.sidebar.slider("Peso Volume", 0.0, 2.0, 1.0, 0.1),
            'cycles': st.sidebar.slider("Peso Ciclos", 0.0, 2.0, 1.0, 0.1),
            'spread_bonus': st.sidebar.slider("Peso Bônus Spread", 0.0, 2.0, 1.0, 0.1),
            'target_proximity': st.sidebar.slider("Peso Proximidade Alvo", 0.0, 2.0, 1.0, 0.1),
            'growth_consistency': st.sidebar.slider("Peso Cresc. Consecutivo", 0.0, 2.0, 1.0, 0.1),
            'momentum': st.sidebar.slider("Peso Momentum", 0.0, 2.0, 1.0, 0.1),
            'acceleration': st.sidebar.slider("Peso Aceleração", 0.0, 2.0, 1.0, 0.1),
        }
        st.sidebar.write("---")

        tabs = st.tabs(["Ações (B3)", "Criptomoedas", "ETFs", "FIIs", "BDRs", "Índices"])
        sheet_names = [SHEET_NAME_STOCKS, SHEET_NAME_CRYPTO, SHEET_NAME_ETFS, SHEET_NAME_FIIS, SHEET_NAME_BDRS]
        asset_types = ["Ações", "Cripto", "ETFs", "FIIs", "BDRs"]

        for i, tab in enumerate(tabs[:-1]):
            with tab:
                st.header(f"Análise de {asset_types[i]} (Aba '{sheet_names[i]}')")
                process_and_display_data(sheet_names[i], asset_types[i], weights)

        with tabs[-1]:
            display_indices_tab()

    except FileNotFoundError:
        st.error(f"❌ Arquivo '{EXCEL_PATH}' não encontrado.")
    except Exception as e:
        st.error(f"❌ Ocorreu um erro: {e}.")
        # Considerar logar o traceback completo para depuração
        import traceback
        traceback.print_exc()

# 6. PONTO DE ENTRADA DA EXECUÇÃO DO SCRIPT
if __name__ == "__main__":
    main()