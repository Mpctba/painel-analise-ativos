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
SHEET_NAME_BDRS = "BDR" # Nova aba para BDRs (conforme solicitado, nome da aba no Excel)

HIDDEN_FILES = ["hidden_cols.txt", "hidden_col.txt"]
DEFAULT_TICKERS_FILE = "default_tickers.txt" # Novo arquivo para tickers padrão

# --- Funções de Carregamento e Busca de Dados ---

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


# --- Funções de Cálculo e Análise ---

def calcular_sr(row):
    """Calcula os pontos de suporte e resistência (SR)."""
    H = row.get("Máxima sexta desde jun/24")
    L = row.get("Mínima sexta desde jun/24")
    C = row.get("Fechamento mais recente")
    if pd.notnull(H) and pd.notnull(L) and pd.notnull(C):
        P = (H + L + C) / 3
        return pd.Series([round(L - 2*(H-P), 2), round(P-(H-L), 2), round(2*P-H, 2), round(P, 2), round(2*P-L, 2), round(P+(H-L), 2), round(H+2*(P-L), 2)])
    return pd.Series([None]*7)

def encontrar_valores_proximos(row):
    """Encontra os níveis de suporte/resistência mais próximos da cotação atual."""
    preco = row.get("Cotação atual")
    niveis = [v for k, v in row.items() if k in ["S3","S2","S1","P","R1","R2","R3"] and pd.notnull(v)]
    niveis.sort()
    abaixo = max([v for v in niveis if v<=preco], default=None)
    acima = min([v for v in niveis if v>preco], default=None)
    return pd.Series([abaixo,acima])

def calcular_distancia_percentual(row):
    """Calcula a menor distância percentual da cotação atual para o nível mais próximo."""
    preco = row.get("Cotação atual")
    abaixo = row.get("Nível abaixo")
    acima = row.get("Nível acima")
    d1 = abs((preco-abaixo)/preco)*100 if pd.notnull(abaixo) and preco != 0 else None
    d2 = abs((acima-preco)/preco)*100 if pd.notnull(acima) and preco != 0 else None
    return round(min([d for d in [d1,d2] if d is not None], default=None), 2) if d1 is not None or d2 is not None else None

def encontrar_var_faixa(row, k_values_list): # Renomeado k_cols para k_values_list para clareza
    """Encontra a faixa de variação (K) em que a variação atual se encaixa."""
    var = row.get("Var")
    arr = sorted([v for v in k_values_list if pd.notnull(v)]) # Agora espera valores, não nomes de coluna
    if pd.notnull(var) and arr:
        aba = max([v for v in arr if v<=var], default=None)
        ac = min([v for v in arr if v > var], default=None)
        return pd.Series([aba, ac])
    return pd.Series([None, None])


def prever_alvo(row, last_cols, last_dates, next_friday):
    """Prevê o valor alvo usando regressão linear simples."""
    ys = [row[c] for c in last_cols]
    # Garante que há pelo menos dois pontos de dados válidos para polyfit
    valid_indices = [i for i, y in enumerate(ys) if pd.notnull(y)]
    if len(valid_indices) < 2:
        return None

    valid_ys = [ys[i] for i in valid_indices]
    valid_xs = [last_dates[i].toordinal() for i in valid_indices]

    if len(valid_xs) < 2: # Verifica novamente após filtrar por dados válidos
        return None

    m, b = np.polyfit(valid_xs, valid_ys, 1)
    return round(m * next_friday.toordinal() + b, 2)

# --- FUNÇÃO MODIFICADA: highlight_colunas_comparadas ---
def highlight_colunas_comparadas(row, colunas_para_estilo):
    """
    Aplica estilo de cor (verde/vermelho) às colunas de cotação
    com base na comparação com o valor anterior.
    VERDE: se for maior OU IGUAL ao anterior.
    VERMELHO: se for menor ao anterior.
    """
    vals = row[colunas_para_estilo].values
    styles = [''] * len(vals)
    for i in range(1, len(vals)):
        ant = vals[i-1]
        atual = vals[i]

        if pd.notnull(ant) and pd.notnull(atual):
            if atual >= ant: # ALTERADO: Condição para verde agora é maior OU IGUAL
                styles[i] = 'color: green; font-weight: bold'
            elif atual < ant:
                styles[i] = 'color: red; font-weight: bold'
            # Else (atual == ant, mas já coberto pelo >=), style permanece '' se não for menor.
    return styles
    
# --- NOVA FUNÇÃO: highlight_insights (para colorir a coluna Insight) ---
def highlight_insights(val):
    if isinstance(val, str):
        if "Compra Forte" in val or "Atenção para Compra" in val:
            return 'color: green; font-weight: bold'
        elif "Evitar / Atenção" in val:
            return 'color: red; font-weight: bold'
    return ''

# --- FUNÇÃO calculate_consecutive_growth (SEM ALTERAÇÕES, pois já conta crescimento ESTRITO para "Nível") ---
def calculate_consecutive_growth(row, static_date_cols, current_quote_col):
    """
    Calcula o número de semanas consecutivas de crescimento (fonte verde),
    contando da cotação mais recente para a mais antiga.
    A "Cotação atual" só estende a sequência se for estritamente maior que a última data estática.
    Caso contrário, a contagem considera apenas as datas estáticas.
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
        elif curr_val > prev_val: # AQUI A CONDIÇÃO É AINDA ESTRITA, como solicitado para o "Nível" anterior
            consecutive_growth_count += 1
        else:
            break

    return consecutive_growth_count

def load_default_tickers(file_path: str, all_options: list) -> list:
    """
    Carrega tickers padrão de um arquivo de texto, filtrando pelos tickers disponíveis.
    """
    default_tickers = []
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                # Filtra e normaliza os tickers lidos do arquivo
                loaded_tickers = [unicodedata.normalize('NFC', line.strip()) for line in f if line.strip()]
                # Retorna apenas os tickers que existem nas opções disponíveis
                default_tickers = [t for t in loaded_tickers if t in all_options]
        except Exception as e:
            st.warning(f"Erro ao carregar tickers padrão do arquivo '{file_path}': {e}")
    return default_tickers

def calculate_historical_spread(hist_data: pd.DataFrame):
    """
    Calcula o 'Spread (%)' para cada dia no histórico de dados.
    H, L, C (máxima sexta, mínima sexta, fechamento mais recente)
    são calculados dinamicamente com base nos dados até o dia atual.
    """
    if hist_data.empty:
        return pd.Series(dtype=float)

    historical_spreads = []
    # Garante que o índice é datetime para fácil filtragem
    hist_data.index = pd.to_datetime(hist_data.index)

    # Definir chaves para reutilização
    sr_keys = ["S3","S2","S1","P","R1","R2","R3"]
    nearest_level_keys = ["Nível abaixo", "Nível acima"]
    var_faixa_keys = ["Var (abaixo)", "Var (acima)"] # Adicionado para clareza

    for i in range(len(hist_data)):
        current_date = hist_data.index[i]

        # Filtra o histórico até a data atual
        daily_hist = hist_data.loc[hist_data.index <= current_date]

        # Calcula H, L, C dinamicamente para o contexto deste dia
        sextas_up_to_date = daily_hist[daily_hist.index.weekday == 4]

        H_D = round(sextas_up_to_date["Close"].max(), 2) if not sextas_up_to_date.empty else None
        L_D = round(sextas_up_to_date["Close"].min(), 2) if not sextas_up_to_date.empty else None

        # C_D é o fechamento da última sexta-feira até a data atual
        C_D = round(sextas_up_to_date["Close"].iloc[-1], 2) if not sextas_up_to_date.empty else None

        # Preço de fechamento e variação do dia atual
        Preco_D = round(daily_hist["Close"].iloc[-1], 2)
        Var_D = None
        if len(daily_hist) >= 2:
            prev_close = daily_hist["Close"].iloc[-2]
            if prev_close != 0:
                Var_D = round(((Preco_D - prev_close) / prev_close) * 100, 2)

        # Cria uma linha temporária para reutilizar as funções de cálculo de SR
        temp_row = {
            "Máxima sexta desde jun/24": H_D,
            "Mínima sexta desde jun/24": L_D,
            "Fechamento mais recente": C_D,
            "Cotação atual": Preco_D,
            "Var": Var_D
        }
        temp_row_series = pd.Series(temp_row)

        # Calcula os pontos SR e atribui à temp_row_series
        sr_points = calcular_sr(temp_row_series)
        for j, key in enumerate(sr_keys):
            if j < len(sr_points):
                temp_row_series[key] = sr_points.iloc[j]

        # Encontra os níveis mais próximos e atribui à temp_row_series
        nearest_levels = encontrar_valores_proximos(temp_row_series)
        for j, key in enumerate(nearest_level_keys):
            if j < len(nearest_levels):
                temp_row_series[key] = nearest_levels.iloc[j]

        # Calcula Delta (Distância Percentual)
        delta = calcular_distancia_percentual(temp_row_series)
        temp_row_series["Delta"] = delta

        # Calcula Amplitude
        amplitude = None
        if pd.notnull(temp_row_series.get("Nível abaixo")) and temp_row_series.get("Nível abaixo") != 0 and pd.notnull(temp_row_series.get("Nível acima")):
            amplitude = round(((temp_row_series.get("Nível acima")/temp_row_series.get("Nível abaixo")-1)*100), 2)
        temp_row_series["Amplitude"] = amplitude

        # Calcula K_cols
        k_div = [-2,-3,-5,-9,-17,-33,-65,65,33,17,9,5,3,2]
        k_values = [round(amplitude/k, 2) if pd.notnull(amplitude) else None for k in k_div]

        # Encontra Var (abaixo) e Var (acima) e atribui à temp_row_series
        var_faixa = encontrar_var_faixa(temp_row_series, k_values)
        for j, key in enumerate(var_faixa_keys): # Usando as chaves explicitamente
            if j < len(var_faixa):
                temp_row_series[key] = var_faixa.iloc[j]

        # Calcula Spread (%)
        spread = None
        if pd.notnull(temp_row_series.get("Var (abaixo)")) and pd.notnull(temp_row_series.get("Var (acima)")):
            spread = round(temp_row_series.get("Var (acima)") - temp_row_series.get("Var (abaixo)"), 2)

        historical_spreads.append(spread)

    return pd.Series(historical_spreads, index=hist_data.index, name="Historical_Spread_Pct")


# --- NOVAS FUNÇÕES DE ANÁLISE DE CICLOS ---

def calculate_fft_period(series: pd.Series, use_log_returns: bool = True) -> float | None:
    """
    Calcula o período dominante usando a Transformada Rápida de Fourier (FFT).
    Retorna o período em unidades de tempo da série (ex: dias).
    """
    if series.empty or len(series.dropna()) < 2:
        return None

    # Usar retornos logarítmicos para estacionariedade
    if use_log_returns:
        data_to_analyze = np.log(series / series.shift(1)).dropna().values
    else:
        data_to_analyze = series.dropna().values

    if len(data_to_analyze) < 2:
        return None

    N = len(data_to_analyze)
    yf = fft(data_to_analyze)
    xf = fftfreq(N, 1) # Frequência de amostragem de 1 unidade de tempo (ex: 1 dia)

    # Amplitudes (apenas a primeira metade é única e relevante)
    amplitudes = 2.0/N * np.abs(yf[0:N//2])
    frequencias = xf[0:N//2]

    # Encontra a frequência dominante (ignorando a frequência 0, que é a componente DC/média)
    # Se todas as amplitudes forem zero (série constante), argmax pode retornar 0
    if np.all(amplitudes[1:] == 0):
        return None # Não há ciclo detectável

    # Verifica se há frequências válidas para evitar erro se amplitudes[1:] for vazio
    if len(amplitudes[1:]) == 0:
        return None

    idx_dominante = np.argmax(amplitudes[1:]) # Ignora o primeiro elemento (frequência 0)
    frequencia_dominante = frequencias[idx_dominante + 1] # +1 para ajustar o índice

    if frequencia_dominante == 0:
        return None # Evita divisão por zero se a frequência dominante for 0

    periodo_ciclo = 1 / frequencia_dominante
    return round(periodo_ciclo, 2)


def calculate_acf_period(series: pd.Series, use_log_returns: bool = True) -> int | None:
    """
    Calcula o período usando a Função de Autocorrelação (ACF).
    Retorna o lag com a maior autocorrelação (excluindo lag 0).
    """
    if series.empty or len(series.dropna()) < 2:
        return None

    # Usar retornos logarítmicos para estacionariedade
    if use_log_returns:
        data_to_analyze = np.log(series / series.shift(1)).dropna().values
    else:
        data_to_analyze = series.dropna().values

    if len(data_to_analyze) < 2:
        return None

    # O número máximo de lags deve ser menor que o comprimento da série
    nlags = min(50, len(data_to_analyze) - 1) # Limita para evitar erros em séries curtas
    if nlags < 1:
        return None

    autocorr_values = acf(data_to_analyze, nlags=nlags, fft=True)

    # Encontra o lag com a maior autocorrelação (ignorando o lag 0)
    # Se todos os valores de autocorrelação forem zero, argmax pode retornar 0
    if np.all(autocorr_values[1:] == 0):
        return None # Não há ciclo detectável

    # Verifica se há lags válidos para evitar erro se autocorr_values[1:] for vazio
    if len(autocorr_values[1:]) == 0:
        return None

    max_autocorr_lag = np.argmax(np.abs(autocorr_values[1:])) + 1 # +1 para ajustar o índice
    return max_autocorr_lag


def calculate_sin_fit_period(series: pd.Series) -> float | None:
    """
    Tenta ajustar uma função senoidal aos dados e retorna o período.
    Pode ser menos robusto para dados financeiros ruidosos.
    """
    data = series.dropna()
    if data.empty or len(data) < 10: # Mínimo de pontos para ajuste
        return None

    x_data = np.arange(len(data))
    y_data = data.values

    # Definir a função senoidal
    def sinusoidal(t, amplitude, periodo, fase, offset):
        return amplitude * np.sin(2 * np.pi * t / periodo + fase) + offset

    # Chutes iniciais para os parâmetros
    # amplitude: estimativa do desvio padrão
    # periodo: um chute razoável, pode ser 20 (dias úteis em um mês) ou 250 (dias úteis em um ano)
    # fase: 0
    # offset: média dos dados
    initial_amplitude = np.std(y_data)
    initial_period = 20 # Chute inicial para um ciclo de 20 dias (aprox. 1 mês útil)
    initial_phase = 0
    initial_offset = np.mean(y_data)

    p0 = [initial_amplitude, initial_period, initial_phase, initial_offset]

    try:
        # Limites para os parâmetros para ajudar na convergência
        # Amplitude > 0, Periodo > 1, Fase qualquer, Offset qualquer
        # Limite superior do período: max 1 ano de dias úteis (252) ou o comprimento dos dados
        bounds = ([0, 1, -np.inf, -np.inf], [np.inf, min(252, len(data)), np.inf, np.inf])

        params, covariance = curve_fit(sinusoidal, x_data, y_data, p0=p0, bounds=bounds, maxfev=5000)

        periodo_fit = params[1]
        if periodo_fit > 0:
            return round(periodo_fit, 2)
        return None
    except RuntimeError as e:
        # print(f"Erro ao ajustar curva senoidal: {e}") # Para depuração
        return None
    except ValueError as e:
        # print(f"Erro de valor ao ajustar curva senoidal: {e}") # Para depuração
        return None

# NOVO: Função para validar a consistência dos ciclos e sugerir um período dominante
def get_dominant_cycle(fft_p, acf_p, sin_p) -> tuple[float | None, str]:
    """
    Avalia a consistência entre os períodos de ciclo e retorna o período dominante
    e uma mensagem de status.
    """
    periods = [p for p in [fft_p, acf_p, sin_p] if pd.notnull(p)]
    
    if not periods:
        return None, "Nenhum ciclo detectado."

    if len(periods) == 1:
        return periods[0], f"Apenas um método de ciclo (Período: {periods[0]:.2f} dias)."

    # Verifica consistência (se 2 ou mais métodos estão próximos, ex: dentro de 20% de diferença)
    consistent_periods = []
    
    # Compara pares de períodos
    if fft_p is not None and acf_p is not None and abs(fft_p - acf_p) / max(fft_p, acf_p) < 0.2:
        consistent_periods.extend([fft_p, acf_p])
    if fft_p is not None and sin_p is not None and abs(fft_p - sin_p) / max(fft_p, sin_p) < 0.2:
        consistent_periods.extend([fft_p, sin_p])
    if acf_p is not None and sin_p is not None and abs(acf_p - sin_p) / max(acf_p, sin_p) < 0.2:
        consistent_periods.extend([acf_p, sin_p])

    if consistent_periods:
        # Pega a média dos períodos consistentes para o período dominante
        dominant_period = round(np.mean(list(set(consistent_periods))), 2) # Usar set para remover duplicatas antes da média
        return dominant_period, f"Período Dominante Consistente: **{dominant_period} dias** (aproximadamente {round(dominant_period / 7, 1)} semanas)."
    else:
        # Caso não haja consistência forte, mas há múltiplos métodos
        return None, "Ciclos detectados, mas sem forte consistência entre os métodos. Pode indicar ruído."


# NOVO: Função para prever alvo baseado em ciclo
def predict_target_from_cycle(hist_data: pd.DataFrame, dominant_cycle_days: float) -> float | None:
    """
    Prevê um alvo futuro baseado no período de ciclo dominante.
    Usa um ajuste senoidal simplificado ou extrapolado do último ciclo.
    Para esta implementação, vamos tentar projetar um pico/vale.
    """
    if hist_data.empty or dominant_cycle_days is None or dominant_cycle_days <= 1 or len(hist_data) < 10: # Ajustei o mínimo para 10
        return None

    data = hist_data['Close'].dropna()
    if data.empty or len(data) < 10: # Ajustei o mínimo para 10
        return None

    x_data = np.arange(len(data))
    y_data = data.values

    # Ajusta uma senoidal aos dados para prever o próximo ponto do ciclo
    def sinusoidal(t, amplitude, fase, offset):
        return amplitude * np.sin(2 * np.pi * t / dominant_cycle_days + fase) + offset

    # Usar o período dominante fornecido
    initial_amplitude = np.std(y_data)
    initial_phase = 0
    initial_offset = np.mean(y_data)

    p0 = [initial_amplitude, initial_phase, initial_offset]

    try:
        # Apenas ajustar amplitude, fase e offset, mantendo o período fixo
        bounds = ([0, -np.inf, -np.inf], [np.inf, np.inf, np.inf])

        # curve_fit agora só com 3 parâmetros
        params, covariance = curve_fit(sinusoidal, x_data, y_data, p0=p0, bounds=bounds, maxfev=5000)
        
        # Projetar 1/4 do ciclo adiante para um possível pico/vale
        projection_days = dominant_cycle_days / 4
        future_x = len(data) + projection_days

        projected_value = sinusoidal(future_x, *params)
        return round(projected_value, 2)
    except Exception as e:
        # st.warning(f"Erro ao projetar alvo por ciclo: {e}") # Para depuração
        return None


# --- calculate_additional_indicators (MODIFICADA) ---
def calculate_additional_indicators(hist_data: pd.DataFrame) -> pd.Series:
    """
    Calcula vários indicadores técnicos para o histórico de dados fornecido.
    Retorna os valores mais recentes de cada indicador.
    """
    if hist_data.empty or len(hist_data) < 20: # Need enough data for rolling windows (e.g., 20 days for MA)
        return pd.Series({
            'Volatilidade_Anualizada': None,
            'SMA_20_Dias': None,
            'EMA_20_Dias': None,
            'BB_Upper': None,
            'BB_Lower': None,
            'RSI_14_Dias': None,
            'Volume_Medio_20_Dias': None,
            'Momentum_10_Dias': None,
            'Aceleracao_10_Dias': None # ADIÇÃO: Aceleração
        })

    # Ensure index is datetime and sorted
    hist_data = hist_data.copy()
    hist_data.index = pd.to_datetime(hist_data.index)
    hist_data = hist_data.sort_index()

    # Volatilidade Anualizada (20 dias, baseada em retornos logarítmicos)
    log_returns = np.log(hist_data['Close'] / hist_data['Close'].shift(1))
    volatilidade_anualizada = log_returns.rolling(window=20).std().iloc[-1] * np.sqrt(252) # 252 trading days in a year
    volatilidade_anualizada = round(volatilidade_anualizada * 100, 2) if pd.notnull(volatilidade_anualizada) else None # Convert to percentage

    # Médias Móveis (20 dias)
    sma_20 = hist_data['Close'].rolling(window=20).mean().iloc[-1]
    ema_20 = hist_data['Close'].ewm(span=20, adjust=False).mean().iloc[-1]

    # Bandas de Bollinger (20 dias, 2 desvios padrão)
    std_dev_20 = hist_data['Close'].rolling(window=20).std().iloc[-1]
    bb_upper = sma_20 + (std_dev_20 * 2) if pd.notnull(sma_20) and pd.notnull(std_dev_20) else None
    bb_lower = sma_20 - (std_dev_20 * 2) if pd.notnull(sma_20) and pd.notnull(std_dev_20) else None

    # RSI (14 dias)
    delta = hist_data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()

    # Evita divisão por zero para RSI
    rs = gain / loss if loss.iloc[-1] != 0 else pd.Series([np.inf])
    rsi_14 = 100 - (100 / (1 + rs))
    rsi_14 = round(rsi_14.iloc[-1], 2) if pd.notnull(rsi_14.iloc[-1]) else None

    # Volume Médio (20 dias)
    volume_medio_20 = hist_data['Volume'].rolling(window=20).mean().iloc[-1]
    volume_medio_20 = round(volume_medio_20, 0) if pd.notnull(volume_medio_20) else None # Round to nearest integer for volume

    # NOVO: Momentum (Preço atual vs. Preço de N dias atrás)
    momentum_period = 10 # Ex: 10 dias
    momentum_10 = None
    if len(hist_data) > momentum_period:
        price_today = hist_data['Close'].iloc[-1]
        price_n_days_ago = hist_data['Close'].iloc[-1 - momentum_period]
        if pd.notnull(price_today) and pd.notnull(price_n_days_ago) and price_n_days_ago != 0:
            momentum_10 = round(((price_today - price_n_days_ago) / price_n_days_ago) * 100, 2)

    # ADIÇÃO: Aceleração (Taxa de crescimento % por dia em média)
    acceleration_period = 10 # Usar o mesmo período do Momentum para consistência
    aceleracao_10 = None
    if len(hist_data) > acceleration_period:
        # Calcula a variação percentual diária
        daily_pct_change = hist_data['Close'].pct_change() * 100
        # Calcula a média da variação diária nos últimos `acceleration_period` dias
        mean_daily_pct_change = daily_pct_change.iloc[-acceleration_period:].mean()
        if pd.notnull(mean_daily_pct_change):
            aceleracao_10 = round(mean_daily_pct_change, 2)

    return pd.Series({
        'Volatilidade_Anualizada': volatilidade_anualizada,
        'SMA_20_Dias': round(sma_20, 2) if pd.notnull(sma_20) else None,
        'EMA_20_Dias': round(ema_20, 2) if pd.notnull(ema_20) else None,
        'BB_Upper': round(bb_upper, 2) if pd.notnull(bb_upper) else None,
        'BB_Lower': round(bb_lower, 2) if pd.notnull(bb_lower) else None,
        'RSI_14_Dias': rsi_14,
        'Volume_Medio_20_Dias': volume_medio_20,
        'Momentum_10_Dias': momentum_10, # Retorna o momentum
        'Aceleracao_10_Dias': aceleracao_10 # ADIÇÃO: Retorna a aceleração
    })

# --- calculate_attractiveness_score (MODIFICADA) ---
def calculate_attractiveness_score(row: pd.Series, weights: dict) -> float:
    """
    Calcula uma pontuação de atratividade de 0 a 10 para um ticker
    com base em vários indicadores, usando pesos configuráveis.
    """
    score = 0.0

    # 1. Volatilidade Anualizada (menor é melhor para atratividade geral)
    vol = row.get('Volatilidade_Anualizada')
    if pd.notnull(vol):
        if vol < 20: # Baixa volatilidade
            score += 1.5 * weights.get('volatility', 1.0)
        elif 20 <= vol < 40: # Média volatilidade
            score += 0.5 * weights.get('volatility', 1.0)
        # else: alta volatilidade (0 pontos)

    # 2. Preço vs. Médias Móveis (tendência de alta)
    close_price = row.get('Cotação atual')
    sma_20 = row.get('SMA_20_Dias')
    ema_20 = row.get('EMA_20_Dias')

    if pd.notnull(close_price) and pd.notnull(sma_20) and close_price > sma_20:
        score += 1.5 * weights.get('moving_averages', 1.0)

    if pd.notnull(close_price) and pd.notnull(ema_20) and close_price > ema_20:
        score += 1.5 * weights.get('moving_averages', 1.0)

    # 3. RSI (condição de sobrevenda)
    rsi = row.get('RSI_14_Dias')
    if pd.notnull(rsi):
        if rsi < 30: # Sobrevendido
            score += 2.5 * weights.get('rsi', 1.0)
        elif 30 <= rsi <= 70: # Neutro
            score += 0.5 * weights.get('rsi', 1.0)
        # else: sobrecomprado (0 pontos)

    # 4. Bandas de Bollinger (preço próximo à banda inferior)
    bb_lower = row.get('BB_Lower')
    if pd.notnull(close_price) and pd.notnull(bb_lower):
        if close_price < bb_lower * 1.01: # Próximo ou abaixo da banda inferior (1% de margem)
            score += 2.0 * weights.get('bollinger_bands', 1.0)
        elif close_price > row.get('BB_Upper', -np.inf) * 0.99: # Próximo ou acima da banda superior
            pass # 0 pontos
        else: # Dentro das bandas
            score += 0.5 * weights.get('bollinger_bands', 1.0)

    # 5. Volume Médio (indica liquidez e interesse)
    volume = row.get('Volume_Medio_20_Dias')
    if pd.notnull(volume) and volume > 0: # Apenas verifica se há volume
        score += 0.5 * weights.get('volume', 1.0)

    # 6. Análise de Ciclos (se um ciclo dominante for detectado)
    dominant_cycle, _ = get_dominant_cycle(row.get('Ciclo_FFT_Dias'), row.get('Ciclo_ACF_Dias'), row.get('Ciclo_Sinoidal_Dias'))
    if dominant_cycle is not None:
        score += 0.5 * weights.get('cycles', 1.0)

    # 7. Bônus para Spread alto com Variação Negativa
    spread_pct = row.get('Spread (%)')
    var_daily = row.get('Var')
    if pd.notnull(spread_pct) and spread_pct > 1.0 and \
       pd.notnull(var_daily) and var_daily < 0:
        score += 1.0 * weights.get('spread_bonus', 1.0) # Adiciona 1.0 ponto de bônus

    # NOVO CRITÉRIO: Proximidade ao Alvo (se o alvo for de alta)
    alvo = row.get('Alvo')
    current_price = row.get('Cotação atual')
    if pd.notnull(alvo) and pd.notnull(current_price) and current_price < alvo: # Alvo de alta
        if alvo > 0: # Evitar divisão por zero
            dist_to_alvo = (alvo - current_price) / current_price * 100
            if dist_to_alvo > 0 and dist_to_alvo <= 5: # Muito próximo do alvo, ainda pode ter upside
                score += 1.0 * weights.get('target_proximity', 1.0)
            elif dist_to_alvo > 5 and dist_to_alvo <= 15: # Distância razoável para upside
                score += 0.5 * weights.get('target_proximity', 1.0)

    # NOVO CRITÉRIO: Consistência de Crescimento (Nível)
    nivel = row.get('Nível')
    if pd.notnull(nivel):
        if nivel >= 3: # 3 ou mais semanas de crescimento
            score += 1.5 * weights.get('growth_consistency', 1.0)
        elif nivel == 2:
            score += 0.5 * weights.get('growth_consistency', 1.0)

    # NOVO CRITÉRIO: Momentum
    momentum = row.get('Momentum_10_Dias')
    if pd.notnull(momentum):
        if momentum > 5: # Forte momentum de alta
            score += 1.0 * weights.get('momentum', 1.0)
        elif momentum > 0: # Momentum de alta
            score += 0.5 * weights.get('momentum', 1.0)

    # ADIÇÃO: Novo critério para Aceleração
    aceleracao = row.get('Aceleracao_10_Dias')
    if pd.notnull(aceleracao):
        if aceleracao > 1.0: # Forte aceleração (crescimento médio diário > 1%)
            score += 1.0 * weights.get('acceleration', 1.0)
        elif aceleracao > 0: # Aceleração positiva
            score += 0.5 * weights.get('acceleration', 1.0)
        # Se for negativa, não adiciona pontos

    # Normaliza para uma escala de 0 a 10
    # O score máximo teórico pode variar com os pesos. Para garantir 0-10, vamos simplesmente limitar.
    return round(min(10.0, score), 2)

# --- NOVA FUNÇÃO: generate_insight (MODIFICADA) ---
def generate_insight(row: pd.Series) -> str:
    """
    Gera uma única string de insight principal com base na classificação de atratividade.
    As informações de Score, Nível e Aceleração são omitidas, pois já têm colunas próprias.
    """
    score = row.get("Score")
    if pd.notnull(score):
        if score >= 8.0:
            return "Compra Forte"
        elif score >= 6.0:
            return "Atenção para Compra"
        elif score >= 4.0:
            return "Monitorar"
        else:
            return "Evitar / Atenção"
    
    # Se o score for nulo, indica que não há dados suficientes para calcular
    return "Sem dados para análise"


# --- NOVA FUNÇÃO: get_max_price_last_12_months ---
def get_max_price_last_12_months(hist_data: pd.DataFrame) -> float | None:
    """
    Calcula o preço máximo (High) do ativo nos últimos 12 meses (aprox. 365 dias).

    Args:
        hist_data (pd.DataFrame): DataFrame com o histórico de preços (deve ter o índice datetime e a coluna 'High').

    Returns:
        float | None: O preço máximo nos últimos 12 meses, ou None se os dados forem insuficientes.
    """
    if hist_data.empty or 'High' not in hist_data.columns:
        return None

    # Certifica-se de que o índice é datetime e está em ordem crescente
    hist_data = hist_data.sort_index(ascending=True)

    # Data mais recente nos dados históricos
    most_recent_data_date = hist_data.index.max()

    # Define a data de início para os últimos 12 meses (365 dias para simplicidade, pode ser ajustado para 252 dias úteis se preferir)
    start_date_12_months_ago = most_recent_data_date - timedelta(days=365)

    # Filtra os dados dos últimos 12 meses
    recent_hist = hist_data.loc[hist_data.index >= start_date_12_months_ago]

    if recent_hist.empty:
        return None

    # Calcula a máxima da coluna 'High' neste período
    max_12_months = recent_hist['High'].max()

    return round(max_12_months, 2) if pd.notnull(max_12_months) else None

# --- NOVA FUNÇÃO: calculate_days_since_target_hit ---
def calculate_days_since_target_hit(hist_data: pd.DataFrame, target_price: float) -> int | None:
    """
    Calcula quantos dias se passaram desde a última vez que o preço de fechamento
    do ativo atingiu ou ultrapassou o preço alvo.

    Args:
        hist_data (pd.DataFrame): DataFrame com o histórico de preços (deve ter a coluna 'Close').
        target_price (float): O preço alvo a ser verificado.

    Returns:
        int | None: O número de dias desde a última vez que o alvo foi atingido/ultrapassado,
                    ou None se o alvo nunca foi atingido ou os dados forem insuficientes.
    """
    # Verifica se hist_data é um DataFrame e não está vazio, e se target_price não é nulo.
    if hist_data is None or hist_data.empty or 'Close' not in hist_data.columns or pd.isna(target_price):
        return None

    # Certifica-se de que o índice é datetime e está em ordem crescente
    hist_data = hist_data.sort_index(ascending=True)

    # Encontra as datas onde o preço de fechamento foi >= ao target_price
    # Usamos >= pois "atingiu ou ultrapassou"
    dates_hit_target = hist_data[hist_data['Close'] >= target_price].index

    if dates_hit_target.empty:
        return None # Nunca atingiu o alvo

    # A última data em que o alvo foi atingido/ultrapassado
    last_hit_date = dates_hit_target.max()

    # Data mais recente disponível no histórico (o "hoje" dos dados)
    most_recent_data_date = hist_data.index.max()

    # Calcula a diferença em dias
    days_since = (most_recent_data_date - last_hit_date).days

    return days_since

# --- NOVA FUNÇÃO: calculate_dividend_yield ---
def calculate_dividend_yield(hist_data: pd.DataFrame, current_price: float) -> float | None:
    """
    Calcula o Dividend Yield (DY) anualizado.
    Soma os dividendos pagos nos últimos 12 meses e divide pela cotação atual.
    """
    if hist_data.empty or 'Dividends' not in hist_data.columns or pd.isna(current_price) or current_price == 0:
        return None

    # Certifica-se de que o índice é datetime e está em ordem crescente
    hist_data = hist_data.sort_index(ascending=True)

    # Data mais recente nos dados históricos
    most_recent_data_date = hist_data.index.max()

    # Define a data de início para os últimos 12 meses (365 dias)
    start_date_12_months_ago = most_recent_data_date - timedelta(days=365)

    # Filtra os dados de dividendos dos últimos 12 meses
    recent_dividends = hist_data.loc[hist_data.index >= start_date_12_months_ago, 'Dividends']

    # Soma os dividendos pagos neste período
    total_dividends_last_12_months = recent_dividends.sum()

    if total_dividends_last_12_months == 0:
        return 0.0 # Se não houve dividendos, o yield é 0

    dy = (total_dividends_last_12_months / current_price) * 100
    return round(dy, 2)

# --- NOVA FUNÇÃO: get_next_ex_dividend_date ---
@st.cache_data(ttl=3600) # Cache por 1 hora
def get_next_ex_dividend_date(ticker_yf: str) -> date | None:
    """
    Busca a próxima data ex-dividendo para um ticker usando yfinance.
    Retorna a data se encontrada e for futura, caso contrário None.
    """
    try:
        ticker = yf.Ticker(ticker_yf)
        actions = ticker.actions
        if actions.empty or 'Dividends' not in actions.columns:
            return None

        dividends = actions[actions['Dividends'] > 0]
        if dividends.empty:
            return None

        # Converte o índice (data) para timezone-naive para comparação com date.today()
        dividends.index = dividends.index.tz_localize(None)

        today_naive = date.today()
        future_dividends = dividends[dividends.index.date >= today_naive].sort_index()

        if not future_dividends.empty:
            return future_dividends.index[0].date()
        else:
            return None
    except Exception as e:
        # print(f"Erro ao buscar próxima data ex-dividendo para {ticker_yf}: {e}")
        return None

# --- NOVA FUNÇÃO: get_last_ex_dividend_date ---
@st.cache_data(ttl=3600) # Cache por 1 hora
def get_last_ex_dividend_date(ticker_yf: str) -> date | None:
    """
    Busca a última data ex-dividendo (passada) para um ticker usando yfinance.
    Retorna a data se encontrada e for passada, caso contrário None.
    """
    try:
        ticker = yf.Ticker(ticker_yf)
        actions = ticker.actions
        if actions.empty or 'Dividends' not in actions.columns:
            return None

        dividends = actions[actions['Dividends'] > 0]
        if dividends.empty:
            return None

        # Converte o índice (data) para timezone-naive para comparação com date.today()
        dividends.index = dividends.index.tz_localize(None)

        today_naive = date.today()
        # Filtra por dividendos passados e pega o mais recente
        past_dividends = dividends[dividends.index.date < today_naive].sort_index(ascending=False)

        if not past_dividends.empty:
            return past_dividends.index[0].date() # Retorna a data mais recente do passado
        else:
            return None # Nenhum dividendo passado encontrado
    except Exception as e:
        # print(f"Erro ao buscar última data ex-dividendo para {ticker_yf}: {e}")
        return None


# --- NOVA FUNÇÃO: visualize_price_data (para gráficos interativos) ---
def visualize_price_data(hist_data: pd.DataFrame, ticker: str, sr_levels: dict, events_df: pd.DataFrame = None):
    """
    Cria um gráfico interativo do histórico de preços com Médias Móveis,
    Bandas de Bollinger, SR levels e eventos de dividendos.
    """
    if hist_data.empty:
        st.warning(f"Não há dados históricos para exibir o gráfico de {ticker}.")
        return

    hist_data = hist_data.copy()
    hist_data.index = pd.to_datetime(hist_data.index)
    
    # Verifica e calcula indicadores novamente, se não estiverem no DataFrame (para visualização)
    # Garante que temos dados suficientes para o cálculo das médias e bandas
    if len(hist_data) >= 20:
        if 'SMA_20_Dias' not in hist_data.columns or 'BB_Upper' not in hist_data.columns:
            # Calcula rolling para o gráfico completo
            hist_data['SMA_20_Dias'] = hist_data['Close'].rolling(window=20).mean()
            hist_data['EMA_20_Dias'] = hist_data['Close'].ewm(span=20, adjust=False).mean()
            hist_data['StdDev'] = hist_data['Close'].rolling(window=20).std()
            hist_data['BB_Upper'] = hist_data['SMA_20_Dias'] + (hist_data['StdDev'] * 2)
            hist_data['BB_Lower'] = hist_data['SMA_20_Dias'] - (hist_data['StdDev'] * 2)
    else:
        st.warning(f"Dados históricos insuficientes para calcular médias móveis e Bandas de Bollinger para {ticker}.")
        hist_data['SMA_20_Dias'] = np.nan
        hist_data['EMA_20_Dias'] = np.nan
        hist_data['BB_Upper'] = np.nan
        hist_data['BB_Lower'] = np.nan


    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.1,
                        row_heights=[0.7, 0.3])

    # Candlestick chart
    fig.add_trace(go.Candlestick(x=hist_data.index,
                                 open=hist_data['Open'],
                                 high=hist_data['High'],
                                 low=hist_data['Low'],
                                 close=hist_data['Close'],
                                 name='Preço'), row=1, col=1)

    # Moving Averages
    if 'SMA_20_Dias' in hist_data.columns and hist_data['SMA_20_Dias'].any():
        fig.add_trace(go.Scatter(x=hist_data.index, y=hist_data['SMA_20_Dias'], line=dict(color='orange', width=1), name='SMA 20'), row=1, col=1)
    if 'EMA_20_Dias' in hist_data.columns and hist_data['EMA_20_Dias'].any():
        fig.add_trace(go.Scatter(x=hist_data.index, y=hist_data['EMA_20_Dias'], line=dict(color='purple', width=1), name='EMA 20'), row=1, col=1)

    # Bollinger Bands
    if 'BB_Upper' in hist_data.columns and hist_data['BB_Upper'].any():
        fig.add_trace(go.Scatter(x=hist_data.index, y=hist_data['BB_Upper'], line=dict(color='gray', width=1, dash='dash'), name='BB Superior'), row=1, col=1)
        fig.add_trace(go.Scatter(x=hist_data.index, y=hist_data['BB_Lower'], line=dict(color='gray', width=1, dash='dash'), name='BB Inferior'), row=1, col=1)

    # Support/Resistance Levels
    colors = ['#FF4136', '#FF851B', '#FFDC00', '#2ECC40', '#0074D9', '#3D9970', '#85144B'] # SR colors
    sr_names = ['S3', 'S2', 'S1', 'P', 'R1', 'R2', 'R3']
    for i, level_name in enumerate(sr_names):
        level_value = sr_levels.get(level_name)
        if level_value is not None and pd.notnull(level_value):
            fig.add_hline(y=level_value, line_dash="dash", line_color=colors[i], annotation_text=level_name, annotation_position="top left", row=1, col=1)

    # Dividend Events
    if events_df is not None and not events_df.empty:
        # Filter for dividends > 0
        dividend_events = events_df[events_df['Dividends'] > 0]
        if not dividend_events.empty:
            # Garante que os índices coincidem para pegar o 'Close' correto
            dividend_dates_in_hist = hist_data.index.intersection(dividend_events.index)
            if not dividend_dates_in_hist.empty:
                fig.add_trace(go.Scatter(
                    x=dividend_dates_in_hist,
                    y=hist_data.loc[dividend_dates_in_hist, 'Close'], # Get close price at dividend date
                    mode='markers',
                    marker=dict(symbol='triangle-up', size=8, color='blue', line=dict(width=1, color='DarkSlateGrey')),
                    name='Dividendos',
                    text=[f"Dividendo: {dividend_events.loc[d, 'Dividends']:.2f}" for d in dividend_dates_in_hist],
                    hoverinfo='text+x+y'
                ), row=1, col=1)
    
    # Volume chart
    if 'Volume' in hist_data.columns and hist_data['Volume'].any():
        fig.add_trace(go.Bar(x=hist_data.index, y=hist_data['Volume'], name='Volume', marker_color='lightblue'), row=2, col=1)

    fig.update_layout(title_text=f'Análise de Preços para {ticker}',
                      xaxis_rangeslider_visible=False,
                      height=600,
                      hovermode='x unified')
    fig.update_yaxes(title_text='Preço', row=1, col=1)
    fig.update_yaxes(title_text='Volume', row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)


# --- Função Principal de Processamento e Exibição de Dados (MODIFICADA) ---

def process_and_display_data(sheet_name: str, asset_type_display_name: str, weights: dict):
    """
    Função auxiliar para processar e exibir os dados para um tipo de ativo específico,
    reduzindo a duplicação de código.
    """
    df = carregar_planilha(EXCEL_PATH, sheet_name)

    # Verifica se o DataFrame carregado está vazio
    if df.empty:
        st.info(f"A planilha '{sheet_name}' para {asset_type_display_name} está vazia ou não pôde ser carregada. Por favor, verifique o arquivo Excel.")
        st.dataframe(pd.DataFrame()) # Exibe um DataFrame vazio
        return # Sai da função se não houver dados

    hidden_cols_raw = []
    for fname in HIDDEN_FILES:
        if os.path.exists(fname):
            with open(fname, "r", encoding="utf-8") as f:
                hidden_cols_raw = [line.strip() for line in f if line.strip()]
            break
    hidden_cols = [unicodedata.normalize('NFC', h) for h in hidden_cols_raw]

    if "Ticker" not in df.columns:
        st.warning(f"A coluna 'Ticker' não foi encontrada na planilha '{sheet_name}'. Certifique-se de que a coluna existe e está nomeada corretamente.")
        st.dataframe(pd.DataFrame()) # Exibe um DataFrame vazio
        return # Sai da função se não houver coluna Ticker

    # Lógica de formatação do Ticker para yFinance
    df["Ticker_YF"] = df["Ticker"].astype(str).str.strip()
    # Adiciona .SA apenas para ações, ETFs, FIIs e BDRs
    if asset_type_display_name in ["Ações", "ETFs", "FIIs", "BDRs"]:
        df["Ticker_YF"] = df["Ticker_YF"] + ".SA"
    # Para criptos com -USD, o ticker já está no formato correto e não há conversão para BRL.

    # Aplica a função para obter os dados do yfinance
    df[[
        "Cotação atual",
        "Var",
        "Mínima sexta desde jun/24",
        "Máxima sexta desde jun/24",
        "Fechamento mais recente",
        "Raw_Hist_Data"
    ]] = df["Ticker_YF"].apply(lambda t: pd.Series(get_price_var_min_max_last(t)))

    # --- Aplicação dos cálculos de Suporte/Resistência e outros indicadores ---
    df[["S3","S2","S1","P","R1","R2","R3"]] = df.apply(calcular_sr, axis=1)
    df[["Nível abaixo","Nível acima"]] = df.apply(encontrar_valores_proximos, axis=1)
    df.rename(columns={"Distância percentual": "Delta"}, inplace=True)
    df["Delta"] = df.apply(calcular_distancia_percentual, axis=1)
    df["Amplitude"] = df.apply(lambda r: round(((r.get("Nível acima")/r.get("Nível abaixo")-1)*100), 2) if pd.notnull(r.get("Nível abaixo")) and r.get("Nível abaixo")!=0 else None, axis=1)

    k_div = [-2,-3,-5,-9,-17,-33,-65,65,33,17,9,5,3,2]
    k_cols = [f"K ({k})" for k in k_div]
    df[k_cols] = df["Amplitude"].apply(lambda amp: pd.Series([round(amp/k, 2) if pd.notnull(amp) else None for k in k_div]))

    # CORREÇÃO APLICADA AQUI: Passa os valores das colunas K, não os nomes das colunas
    df[["Var (abaixo)","Var (acima)"]] = df.apply(lambda row: encontrar_var_faixa(row, [row[c] for c in k_cols]), axis=1)
    df["Spread (%)"] = df.apply(lambda r: round(r.get("Var (acima)")-r.get("Var (abaixo)"), 2) if pd.notnull(r.get("Var (abaixo)")) and pd.notnull(r.get("Var (acima)")) else None, axis=1)

    date_cols = [c for c in df.columns if c[:4].isdigit() and "-" in c]
    for c in date_cols: df[c] = pd.to_numeric(df[c],errors="coerce")

    today = date.today()
    wd = today.weekday()
    offset = (4 - wd) % 7
    offset = offset if offset != 0 else 7
    next_friday = today + timedelta(days=offset)

    # Garante que há colunas de data suficientes para a predição, caso contrário, pula o cálculo de Alvo
    if len(date_cols) >= 4: # Precisa de pelo menos 4 para last_cols[-4:]
        last_cols = date_cols[-4:]
        last_dates = []
        for col in last_cols:
            try:
                d = dt.fromisoformat(str(col))
            except ValueError:
                d = pd.to_datetime(col)
            last_dates.append(d.date())
        df['Alvo'] = df.apply(lambda row: prever_alvo(row, last_cols, last_dates, next_friday), axis=1)
    else:
        df['Alvo'] = None # Define Alvo como None se não houver dados históricos suficientes para a predição
        st.warning(f"Não há colunas de data suficientes na planilha '{sheet_name}' para o cálculo do 'Alvo' (mínimo de 4 colunas de data necessárias).")

    # NOVO: Calcula a máxima dos últimos 12 meses para cada ativo
    df['Maxima_12_Meses'] = df['Raw_Hist_Data'].apply(get_max_price_last_12_months)

    # NOVO CÁLCULO: Dias desde a última vez que o ativo atingiu/ultrapassou o Preço Alvo, ou "Máxima"
    df['Dias_Alvo'] = df.apply( # *** ALTERADO AQUI: 'Dias_Desde_Alvo' para 'Dias_Alvo' ***
        lambda row: "Máxima"
        if pd.notnull(row["Alvo"]) and pd.notnull(row["Maxima_12_Meses"]) and row["Alvo"] >= row["Maxima_12_Meses"]
        else (
            calculate_days_since_target_hit(row["Raw_Hist_Data"], row["Alvo"])
            if row["Raw_Hist_Data"] is not None and not row["Raw_Hist_Data"].empty and pd.notnull(row["Alvo"])
            else None
        ),
        axis=1
    )


    # --- Cálculo do Nível ---
    static_price_cols_for_growth = sorted([c for c in df.columns if c[:4].isdigit() and "-" in c])[-5:]

    df['Nível'] = df.apply(
        lambda row: calculate_consecutive_growth(row, static_price_cols_for_growth, "Cotação atual"), axis=1
    )

    # Filtro de ticker e exibição do DataFrame
    opt = df["Ticker"].unique().tolist()

    # Carrega os tickers padrão para a aba atual
    default_selected = load_default_tickers(DEFAULT_TICKERS_FILE, opt)

    sel = st.multiselect(f"Filtrar por Ticker ({asset_type_display_name}):", options=opt, default=default_selected, key=f"multiselect_{asset_type_display_name}")

    # Se nenhum ticker for selecionado, o DataFrame pode se tornar vazio.
    # Adiciona uma verificação explícita aqui para lidar com isso.
    if sel:
        df = df[df["Ticker"].isin(sel)]

    if df.empty:
        st.info(f"Nenhum Ticker selecionado ou nenhum dado disponível para a aba '{asset_type_display_name}'.")
        st.dataframe(pd.DataFrame()) # Exibe um DataFrame vazio
        return # Sai da função

    # --- Seção de Análise de Ciclos ---
    st.subheader(f"📊 Análise de Ciclos ({asset_type_display_name})")
    perform_cycle_analysis = st.checkbox(f"Realizar Análise de Ciclos para {asset_type_display_name}", key=f"perform_cycle_analysis_{asset_type_display_name}")

    if perform_cycle_analysis:
        use_log_returns_for_cycle = st.checkbox(
            "Usar Retornos Logarítmicos para Análise de Ciclos (Recomendado para FFT/ACF)",
            value=True, key=f"log_returns_cycle_{asset_type_display_name}"
        )
        st.info("A análise de ciclos pode levar alguns segundos, especialmente para históricos longos.")

        # Inicializa colunas de ciclo com None
        df['Ciclo_FFT_Dias'] = None
        df['Ciclo_ACF_Dias'] = None
        df['Ciclo_Sinoidal_Dias'] = None

        with st.spinner(f"Calculando ciclos para {asset_type_display_name}..."):
            for index, row in df.iterrows():
                hist_data = row["Raw_Hist_Data"]

                if hist_data is None or hist_data.empty:
                    # st.warning(f"Dados históricos brutos não disponíveis ou vazios para {row['Ticker']}. Pulando análise de ciclos.")
                    continue # Pula para o próximo ticker

                close_prices = hist_data["Close"]

                if len(close_prices.dropna()) < 10: # Ajuste este valor conforme a necessidade mínima para seus cálculos de ciclo
                    # st.info(f"Dados históricos insuficientes para análise de ciclo para {row['Ticker']} ({len(close_prices.dropna())} pontos). Mínimo de 10 pontos recomendados.")
                    continue # Pula para o próximo ticker

                # Calcula Ciclo FFT
                fft_period = calculate_fft_period(close_prices, use_log_returns=use_log_returns_for_cycle)
                df.loc[index, 'Ciclo_FFT_Dias'] = fft_period

                # Calcula Ciclo ACF
                acf_period = calculate_acf_period(close_prices, use_log_returns=use_log_returns_for_cycle)
                df.loc[index, 'Ciclo_ACF_Dias'] = acf_period

                # Calcula Ciclo Sinoidal
                sin_period = calculate_sin_fit_period(close_prices)
                df.loc[index, 'Ciclo_Sinoidal_Dias'] = sin_period

        # NOVO: Calcula o período dominante e a mensagem
        df[['Ciclo_Dominante_Dias', 'Status_Ciclo']] = df.apply(
            lambda row: pd.Series(get_dominant_cycle(row['Ciclo_FFT_Dias'], row['Ciclo_ACF_Dias'], row['Ciclo_Sinoidal_Dias'])),
            axis=1
        )
        # NOVO: Projeta alvo com base no ciclo dominante
        df['Alvo_Ciclo'] = df.apply(
            lambda row: predict_target_from_cycle(row['Raw_Hist_Data'], row['Ciclo_Dominante_Dias'])
            if pd.notnull(row['Ciclo_Dominante_Dias']) else None,
            axis=1
        )

        # --- SAÍDAS DE DEBUG PARA VERIFICAR AS COLUNAS DE CICLO ---
        st.subheader(f"🔍 Debug: Colunas de Ciclo Calculadas ({asset_type_display_name})")
        # Filtra apenas as colunas relevantes para debug
        debug_cycle_cols_df = df[['Ticker', 'Ciclo_FFT_Dias', 'Ciclo_ACF_Dias', 'Ciclo_Sinoidal_Dias', 'Ciclo_Dominante_Dias', 'Status_Ciclo', 'Alvo_Ciclo']].copy()
        st.dataframe(debug_cycle_cols_df)
        st.info("Se as colunas acima estiverem vazias, significa que os dados históricos para esses tickers podem ser muito curtos ou constantes para a detecção de ciclos.")
        # --- FIM DAS SAÍDAS DE DEBUG ---

        # NOVO: Visualização dos ciclos para um ticker selecionado (na depuração de gráficos)
        if len(sel) == 1:
            st.subheader(f"📈 Visualização Detalhada do Ciclo para {sel[0]}")
            selected_ticker_row = df[df["Ticker"] == sel[0]].iloc[0]
            hist_data_for_plot = selected_ticker_row["Raw_Hist_Data"]
            dominant_cycle_p = selected_ticker_row['Ciclo_Dominante_Dias']

            if hist_data_for_plot is not None and not hist_data_for_plot.empty and dominant_cycle_p is not None:
                x_data_plot = np.arange(len(hist_data_for_plot.index))
                y_data_plot = hist_data_for_plot['Close'].values.astype(float) # Ensure float

                # Fit sinusoidal function for plotting based on detected dominant cycle
                def sinusoidal_plot(t, amplitude, fase, offset):
                    return amplitude * np.sin(2 * np.pi * t / dominant_cycle_p + fase) + offset

                try:
                    # Chutes iniciais para o plot
                    p0_plot = [np.std(y_data_plot), 0, np.mean(y_data_plot)]
                    bounds_plot = ([0, -np.inf, -np.inf], [np.inf, np.inf, np.inf])

                    params_plot, _ = curve_fit(sinusoidal_plot, x_data_plot, y_data_plot, p0=p0_plot, bounds=bounds_plot, maxfev=5000)
                    fitted_curve = sinusoidal_plot(x_data_plot, *params_plot)

                    fig_cycle = go.Figure()
                    fig_cycle.add_trace(go.Scatter(x=hist_data_for_plot.index, y=hist_data_for_plot['Close'], mode='lines', name='Preço de Fechamento'))
                    fig_cycle.add_trace(go.Scatter(x=hist_data_for_plot.index, y=fitted_curve, mode='lines', name=f'Ajuste Senoidal (Período {dominant_cycle_p:.2f} dias)', line=dict(color='red', dash='dash')))
                    fig_cycle.update_layout(title=f"Ajuste Senoidal para {sel[0]}", xaxis_title="Data", yaxis_title="Preço")
                    st.plotly_chart(fig_cycle, use_container_width=True)

                    # Plot ACF
                    if use_log_returns_for_cycle:
                        data_for_acf_plot = np.log(hist_data_for_plot['Close'] / hist_data_for_plot['Close'].shift(1)).dropna()
                    else:
                        data_for_acf_plot = hist_data_for_plot['Close'].dropna()
                    
                    if len(data_for_acf_plot) > 1:
                        fig_acf = go.Figure()
                        acf_values, confint = acf(data_for_acf_plot, nlags=min(50, len(data_for_acf_plot)-1), alpha=0.05, fft=True)
                        lags = np.arange(len(acf_values))

                        fig_acf.add_trace(go.Bar(x=lags, y=acf_values, name='Autocorrelação'))
                        # Adiciona linhas de confiança
                        fig_acf.add_trace(go.Scatter(x=lags, y=confint[:, 0] - acf_values, mode='lines', name='Lower CI', line=dict(color='gray', dash='dot')))
                        fig_acf.add_trace(go.Scatter(x=lags, y=confint[:, 1] - acf_values, mode='lines', name='Upper CI', line=dict(color='gray', dash='dot')))

                        fig_acf.update_layout(title=f"Função de Autocorrelação (ACF) para {sel[0]}", xaxis_title="Lag (Dias)", yaxis_title="Autocorrelação")
                        st.plotly_chart(fig_acf, use_container_width=True)

                    # Plot FFT (Periodograma)
                    if use_log_returns_for_cycle:
                        data_for_fft_plot = np.log(hist_data_for_plot['Close'] / hist_data_for_plot['Close'].shift(1)).dropna().values
                    else:
                        data_for_fft_plot = hist_data_for_plot['Close'].dropna().values
                    
                    if len(data_for_fft_plot) > 1:
                        N = len(data_for_fft_plot)
                        yf = fft(data_for_fft_plot)
                        xf = fftfreq(N, 1)[:N//2]
                        amplitudes = 2.0/N * np.abs(yf[0:N//2])

                        fig_fft = go.Figure()
                        fig_fft.add_trace(go.Bar(x=1/xf[1:] if len(xf) > 1 and xf[1:].any() else [], y=amplitudes[1:], name='Amplitude')) # Ignora freq 0
                        fig_fft.update_layout(title=f"Periodograma (FFT) para {sel[0]}", xaxis_title="Período (Dias)", yaxis_title="Amplitude")
                        st.plotly_chart(fig_fft, use_container_width=True)


                except Exception as e:
                    st.warning(f"Não foi possível gerar gráficos de ciclo para {sel[0]}: {e}")
            else:
                st.info(f"Dados insuficientes ou nenhum ciclo dominante detectado para gerar gráficos de ciclo para {sel[0]}.")


    # --- NOVO: Aplica os cálculos de indicadores adicionais ---
    indicator_cols = [
        'Volatilidade_Anualizada', 'SMA_20_Dias', 'EMA_20_Dias',
        'BB_Upper', 'BB_Lower', 'RSI_14_Dias', 'Volume_Medio_20_Dias', 'Momentum_10_Dias',
        'Aceleracao_10_Dias' # ADIÇÃO: Inclui a nova coluna
    ]
    df[indicator_cols] = df["Raw_Hist_Data"].apply(calculate_additional_indicators)

    # --- NOVO: Calcula a pontuação de atratividade ---
    # Passa os pesos configurados do sidebar para a função de score
    df['Score'] = df.apply(lambda row: calculate_attractiveness_score(row, weights), axis=1) # Renomeado para 'Score'
    indicator_cols.append('Score') # Adiciona ao controle de colunas
    
    # --- NOVO: Criar a coluna "Insight" ---
    df['Insight'] = df.apply(generate_insight, axis=1)

    # --- NOVO: Calcula o Dividend Yield (DY) ---
    df['DY (%)'] = df.apply(
        lambda row: calculate_dividend_yield(row["Raw_Hist_Data"], row["Cotação atual"]),
        axis=1
    )

    # --- NOVO: Calcula a próxima data ex-dividendo ---
    # Aplica get_next_ex_dividend_date apenas para Ações, FIIs e BDRs
    if asset_type_display_name in ["Ações", "FIIs", "BDRs"]:
        st.info(f"Buscando próxima data ex-dividendo para {asset_type_display_name}...")
        df['Data_ex'] = df['Ticker_YF'].apply(get_next_ex_dividend_date)
    else:
        df['Data_ex'] = None # Define como None para outros tipos de ativos

    # --- NOVO: Calcula a última data ex-dividendo (implementação aqui) ---
    if asset_type_display_name in ["Ações", "FIIs", "BDRs"]:
        st.info(f"Buscando última data ex-dividendo para {asset_type_display_name}...")
        df['Ultima_Data_ex'] = df['Ticker_YF'].apply(get_last_ex_dividend_date)
    else:
        df['Ultima_Data_ex'] = None

    # --- Lógica de ocultar colunas ajustada para reconhecer hidden_cols.txt ---
    ocultar = [col for col in hidden_cols if col in df.columns] + ["Raw_Hist_Data", "Maxima_12_Meses", "Ticker_YF"]
    if not perform_cycle_analysis:
        ocultar.extend(['Ciclo_FFT_Dias', 'Ciclo_ACF_Dias', 'Ciclo_Sinoidal_Dias', 'Ciclo_Dominante_Dias', 'Status_Ciclo', 'Alvo_Ciclo'])

    display_df = df.drop(columns=ocultar, errors="ignore")

    if display_df.empty:
        st.info(f"Nenhum dado para exibir na aba '{asset_type_display_name}' após a remoção de colunas. Por favor, verifique a planilha ou as colunas ocultas.")
        st.dataframe(pd.DataFrame())
        return

    # --- Lógica de reordenação de colunas (CORRIGIDA E MAIS ROBUSTA) ---
    all_cols = list(display_df.columns)

    # Definir a ordem desejada para as colunas
    desired_order_base = ["Ticker", "Insight", "Score", "Spread (%)", "Var", "Nível"]
    
    # Obter as colunas de data e ordená-las
    date_cols_in_df = sorted([c for c in all_cols if c[:4].isdigit() and "-" in c])

    # Definir as colunas que vêm depois de 'Cotação atual'
    explicit_placement_cols = [
        "Alvo", "Alvo_Ciclo", "Dias_Alvo", "DY (%)", "Ultima_Data_ex", "Data_ex",
        'Volatilidade_Anualizada', 'SMA_20_Dias', 'EMA_20_Dias',
        'BB_Upper', 'BB_Lower', 'RSI_14_Dias', 'Volume_Medio_20_Dias',
        'Momentum_10_Dias', 'Aceleracao_10_Dias',
        'Ciclo_FFT_Dias', 'Ciclo_ACF_Dias', 'Ciclo_Sinoidal_Dias', 'Ciclo_Dominante_Dias', 'Status_Ciclo'
    ]

    final_ordered_cols = []

    # 1. Adicionar colunas iniciais (Ticker, Insight, Score, etc.)
    for col in desired_order_base:
        if col in all_cols:
            final_ordered_cols.append(col)

    # 2. Adicionar as colunas de data em ordem cronológica
    final_ordered_cols.extend(date_cols_in_df)

    # 3. Inserir a coluna 'Cotação atual' na posição correta
    if "Cotação atual" in all_cols:
        try:
            # Tenta encontrar a posição da coluna de data específica
            index_2025_07_26 = final_ordered_cols.index("2025-07-26")
            final_ordered_cols.insert(index_2025_07_26 + 1, "Cotação atual")
        except ValueError:
            # Se a coluna '2025-07-26' não existir, tenta inserir antes da coluna 'Alvo'
            if "Alvo" in all_cols:
                try:
                    index_alvo = final_ordered_cols.index("Alvo")
                    final_ordered_cols.insert(index_alvo, "Cotação atual")
                except ValueError:
                    # Se 'Alvo' também não existir, adiciona no final
                    final_ordered_cols.append("Cotação atual")
            else:
                final_ordered_cols.append("Cotação atual")
    
    # 4. Adicionar as demais colunas na ordem desejada
    for col in explicit_placement_cols:
        if col in all_cols and col not in final_ordered_cols:
            final_ordered_cols.append(col)

    # 5. Adicionar quaisquer colunas restantes
    remaining_unsorted_cols = [col for col in all_cols if col not in final_ordered_cols]
    final_ordered_cols.extend(remaining_unsorted_cols)

    display_df = display_df[final_ordered_cols]
    # --- Fim da lógica de reordenação ---

    # --- NOVO: Filtros Avançados ---
    st.subheader(f"⚙️ Filtros Avançados ({asset_type_display_name})")
    col_filters = st.columns(3)
    
    with col_filters[0]:
        min_score = st.slider("Score Mínimo", 0.0, 10.0, 0.0, 0.1, key=f"min_score_{sheet_name}")
        if 'Score' in display_df.columns:
            display_df = display_df[display_df['Score'] >= min_score]
    
    with col_filters[1]:
        min_dy = st.slider("DY (%) Mínimo", 0.0, 10.0, 0.0, 0.1, key=f"min_dy_{sheet_name}")
        if 'DY (%)' in display_df.columns:
            display_df = display_df[display_df['DY (%)'] >= min_dy]

    with col_filters[2]:
        max_vol = st.slider("Volatilidade Anualizada (%) Máxima", 0.0, 100.0, 100.0, 1.0, key=f"max_vol_{sheet_name}")
        if 'Volatilidade_Anualizada' in display_df.columns:
            display_df = display_df[display_df['Volatilidade_Anualizada'] <= max_vol]

    if 'Nível' in display_df.columns:
        min_nivel = st.slider("Nível Mínimo (Crescimento Consecutivo)", 0, 5, 0, 1, key=f"min_nivel_{sheet_name}")
        display_df = display_df[display_df['Nível'] >= min_nivel]

    # --- Fim dos Filtros Avançados ---


    fmt = {col: "{:.2f}" for col in display_df.select_dtypes(include=[np.number]).columns}
    # Formata a nova coluna como inteiro, pois é um nível de 0 a 5
    if 'Nível' in fmt:
        fmt['Nível'] = "{:.0f}"
    # Formata as novas colunas de ciclo como float com 2 casas decimais
    if 'Ciclo_FFT_Dias' in fmt:
        fmt['Ciclo_FFT_Dias'] = "{:.2f}"
    if 'Ciclo_ACF_Dias' in fmt:
        fmt['Ciclo_ACF_Dias'] = "{:.0f}" # Período ACF é um lag, então inteiro
    if 'Ciclo_Sinoidal_Dias' in fmt:
        fmt['Ciclo_Sinoidal_Dias'] = "{:.2f}"
    if 'Ciclo_Dominante_Dias' in fmt:
        fmt['Ciclo_Dominante_Dias'] = "{:.2f}"
    if 'Alvo_Ciclo' in fmt:
        fmt['Alvo_Ciclo'] = "{:.2f}"
    # Formata as novas colunas de indicadores
    if 'Volatilidade_Anualizada' in fmt:
        fmt['Volatilidade_Anualizada'] = "{:.2f}%"
    for col in ['SMA_20_Dias', 'EMA_20_Dias', 'BB_Upper', 'BB_Lower']:
        if col in fmt:
            fmt[col] = "{:.2f}"
    if 'RSI_14_Dias' in fmt:
        fmt['RSI_14_Dias'] = "{:.2f}"
    if 'Volume_Medio_20_Dias' in fmt:
        fmt['Volume_Medio_20_Dias'] = "{:.0f}" # Volume é geralmente um número inteiro
    if 'Momentum_10_Dias' in fmt:
        fmt['Momentum_10_Dias'] = "{:.2f}%"
    # ADIÇÃO: Formatação para Aceleracao_10_Dias
    if 'Aceleracao_10_Dias' in fmt:
        fmt['Aceleracao_10_Dias'] = "{:.2f}%"
    if 'Score' in fmt: # Renomeado para 'Score'
        fmt['Score'] = "{:.1f}" # Score com uma casa decimal
    # NOVA FORMATAÇÃO: Para a coluna 'DY (%)'
    if 'DY (%)' in fmt:
        fmt['DY (%)'] = "{:.2f}%"

    # NOVA FORMATAÇÃO: Para a coluna 'Dias_Alvo' que agora pode ter string "Máxima"
    if 'Dias_Alvo' in fmt:
        # Define uma função de formatação que verifica o tipo
        def format_days_since_alvo(val):
            if pd.notnull(val):
                if isinstance(val, (int, float)):
                    return f"{int(val):.0f}" # Formata como inteiro se for numérico
                else:
                    return str(val) # Caso contrário, retorna como string (ex: "Máxima")
            return '' # Retorna vazio para valores nulos
        fmt['Dias_Alvo'] = format_days_since_alvo

    # NOVA FORMATAÇÃO: Para a coluna 'Data_ex' e 'Ultima_Data_ex'
    if 'Data_ex' in fmt:
        fmt['Data_ex'] = lambda val: val.strftime('%Y-%m-%d') if pd.notnull(val) else ''
    if 'Ultima_Data_ex' in fmt: # Adicionada formatação para a nova coluna
        fmt['Ultima_Data_ex'] = lambda val: val.strftime('%Y-%m-%d') if pd.notnull(val) else ''
    
    # NOVA FORMATAÇÃO: Para a nova coluna 'Insight'
    if 'Insight' in fmt:
        fmt['Insight'] = lambda val: str(val)

    # Reordena o DataFrame com a nova lista de colunas
    # Nota: a variável `final_ordered_cols` foi gerada na etapa de análise acima.
    # Essa seção usa a lista finalizada para reordenar o DataFrame.
    
    # Definir a ordem desejada para as colunas
    all_cols = list(display_df.columns)
    desired_order_base = ["Ticker", "Insight", "Score", "Spread (%)", "Var", "Nível"]
    date_cols_in_df = sorted([c for c in all_cols if c[:4].isdigit() and "-" in c])

    explicit_placement_cols = [
        "Alvo", "Alvo_Ciclo", "Dias_Alvo", "DY (%)", "Ultima_Data_ex", "Data_ex",
        'Volatilidade_Anualizada', 'SMA_20_Dias', 'EMA_20_Dias',
        'BB_Upper', 'BB_Lower', 'RSI_14_Dias', 'Volume_Medio_20_Dias',
        'Momentum_10_Dias', 'Aceleracao_10_Dias',
        'Ciclo_FFT_Dias', 'Ciclo_ACF_Dias', 'Ciclo_Sinoidal_Dias', 'Ciclo_Dominante_Dias', 'Status_Ciclo'
    ]

    final_ordered_cols = []
    for col in desired_order_base:
        if col in all_cols:
            final_ordered_cols.append(col)
    
    final_ordered_cols.extend(date_cols_in_df)
    
    if "Cotação atual" in all_cols:
        try:
            index_2025_07_26 = final_ordered_cols.index("2025-07-26")
            final_ordered_cols.insert(index_2025_07_26 + 1, "Cotação atual")
        except ValueError:
            if "Alvo" in all_cols:
                try:
                    index_alvo = final_ordered_cols.index("Alvo")
                    final_ordered_cols.insert(index_alvo, "Cotação atual")
                except ValueError:
                    final_ordered_cols.append("Cotação atual")
            else:
                final_ordered_cols.append("Cotação atual")

    for col in explicit_placement_cols:
        if col in all_cols and col not in final_ordered_cols:
            final_ordered_cols.append(col)

    remaining_unsorted_cols = [col for col in all_cols if col not in final_ordered_cols]
    final_ordered_cols.extend(remaining_unsorted_cols)

    display_df = display_df[final_ordered_cols]
    
    colunas_para_estilo = [c for c in final_ordered_cols if c[:4].isdigit() and "-" in c] + ["Cotação atual"]
    
    if not display_df.empty and colunas_para_estilo:
        styled = display_df.style.format(fmt)
        styled = styled.apply(lambda row: highlight_colunas_comparadas(row, colunas_para_estilo), axis=1, subset=colunas_para_estilo)
        # Aplica a nova função de estilo para a coluna Insight
        if 'Insight' in display_df.columns:
            styled = styled.applymap(highlight_insights, subset=['Insight'])
        st.dataframe(styled, use_container_width=True)
    else:
        st.dataframe(display_df.style.format(fmt), use_container_width=True)

    # --- REMOÇÃO DA SEÇÃO DE INSIGHTS E RECOMENDAÇÕES ---
    # A lógica de insights agora está na coluna "Insight" da tabela principal.

    # Seção de Depuração de Dados de Sexta-feira
    st.subheader(f"🛠️ Histórico de Dados de Sexta-feira ({asset_type_display_name})")
    debug_friday_data = st.checkbox(f"Exibir dados brutos de Sexta-feira para depuração ({asset_type_display_name}) (selecione apenas um Ticker)", key=f"debug_friday_{asset_type_display_name}")

    if debug_friday_data and len(sel) == 1:
        ticker_to_debug_base = sel[0]
        # Pega a linha original do DataFrame 'df' (não display_df) para ter o Raw_Hist_Data
        selected_row_original = df[df["Ticker"] == ticker_to_debug_base]
        if not selected_row_original.empty and "Raw_Hist_Data" in selected_row_original.columns:
            hist_data = selected_row_original["Raw_Hist_Data"].iloc[0]
            if hist_data is not None and not hist_data.empty:
                sextas_debug = hist_data[hist_data.index.weekday == 4]
                if not sextas_debug.empty:
                    st.write(f"Cotações de Fechamento de Sexta-feira para {sel[0]} desde 2024-06-01:")
                    st.dataframe(sextas_debug[["Close"]])
                    st.write(f"Mínima calculada a partir desses dados: {round(sextas_debug['Close'].min(), 2)}")
                    st.write(f"Máxima calculada a partir desses dados: {round(sextas_debug['Close'].max(), 2)}")
                    st.write(f"Data mínima no histórico completo (após filtro): {hist_data.index.min().date()}")
                    st.write(f"Data máxima no histórico completo (após filtro): {hist_data.index.max().date()}")
                else:
                    st.info(f"Nenhum dado de sexta-feira encontrado para {sel[0]} no período.")
            else:
                st.warning(f"Dados históricos para {sel[0]} não disponíveis ou vazios para depuração. Tente recarregar a página.")
        else:
            st.warning(f"Dados históricos para {sel[0]} não disponíveis para depuração. Tente recarregar a página.")
    elif debug_friday_data and len(sel) != 1:
        st.info("Por favor, selecione exatamente um Ticker para exibir os dados brutos de sexta-feira.")

    # Nova seção de depuração para o cálculo do Alvo
    st.subheader(f"🔍 Depuração do Cálculo do Alvo ({asset_type_display_name})")
    debug_alvo_calc = st.checkbox(f"Exibir detalhes do cálculo do Alvo ({asset_type_display_name}) (selecione apenas um Ticker)", key=f"debug_alvo_{asset_type_display_name}")

    if debug_alvo_calc and len(sel) == 1:
        ticker_to_debug_base = sel[0]
        selected_row = df[df["Ticker"] == ticker_to_debug_base].iloc[0] # Pega a primeira (e única) linha selecionada

        st.write(f"**Detalhes para o Ticker:** {ticker_to_debug_base}")
        st.write(f"**Colunas de data identificadas:** {date_cols}")

        if len(date_cols) >= 4:
            last_cols_debug = date_cols[-4:]
            last_dates_debug = []
            for col in last_cols_debug:
                try:
                    d = dt.fromisoformat(str(col))
                except ValueError:
                    d = pd.to_datetime(col)
                last_dates_debug.append(d.date())

            st.write(f"**Últimas 4 colunas para cálculo do Alvo:** {last_cols_debug}")
            st.write(f"**Datas correspondentes:** {[d.strftime('%Y-%m-%d') for d in last_dates_debug]}")

            ys_debug = [selected_row[c] for c in last_cols_debug]
            st.write(f"**Valores (ys) das colunas de data:** {ys_debug}")

            valid_indices_debug = [i for i, y in enumerate(ys_debug) if pd.notnull(y)] # Corrigido para usar ys_debug
            st.write(f"**Índices de valores válidos para regressão:** {valid_indices_debug}")

            if len(valid_indices_debug) < 2:
                st.warning(f"Atenção: Menos de 2 valores válidos encontrados para o cálculo do Alvo. Isso resultará em 'None'. Verifique se as células nas colunas de data do Excel para este Ticker estão preenchidas e são numéricas.")
            else:
                st.info("Valores válidos suficientes para o cálculo do Alvo.")
        else:
            st.warning(f"Atenção: Não há colunas de data suficientes na planilha '{sheet_name}' para o cálculo do Alvo. Mínimo de 4 colunas de data necessárias.")
    elif debug_alvo_calc and len(sel) != 1:
        st.info("Por favor, selecione exatamente um Ticker para exibir os detalhes do cálculo do Alvo.")

    # Nova seção de Análise do Ciclo do Spread
    st.subheader(f"📊 Análise do Ciclo do Spread (%) ({asset_type_display_name})")
    analyze_spread_cycle = st.checkbox(f"Analisar ciclo do Spread (%) para {asset_type_display_name} (selecione apenas um Ticker)", key=f"analyze_spread_cycle_{asset_type_display_name}")

    if analyze_spread_cycle and len(sel) == 1:
        ticker_to_analyze = sel[0]
        selected_row_original = df[df["Ticker"] == ticker_to_analyze]

        if not selected_row_original.empty and "Raw_Hist_Data" in selected_row_original.columns:
            hist_data = selected_row_original["Raw_Hist_Data"].iloc[0]

            if hist_data is not None and not hist_data.empty:
                st.write(f"Calculando Spread (%) histórico para {ticker_to_analyze}...")

                # Exibe um spinner enquanto o cálculo é feito
                with st.spinner("Isso pode levar alguns segundos para históricos longos..."):
                    historical_spread_series = calculate_historical_spread(hist_data.copy()) # Use uma cópia para evitar modificar o DataFrame em cache

                if not historical_spread_series.empty:
                    # Identifica períodos consecutivos onde o spread estava acima de 1%
                    is_above_1 = (historical_spread_series > 1.0)

                    # Agrupa valores True consecutivos
                    groups = is_above_1.astype(int).groupby((is_above_1 != is_above_1.shift()).cumsum())

                    durations = []
                    for name, group in groups:
                        if group.iloc[0] == True: # Considera apenas grupos onde o spread estava acima de 1%
                            durations.append(len(group))

                    if durations:
                        avg_duration = np.mean(durations)
                        st.write(f"**Média de dias consecutivos com Spread (%) acima de 1% para {ticker_to_analyze}:** {round(avg_duration, 2)} dias.")
                        st.write("---")
                        st.write("**Histórico de Spread (%) (últimos 30 dias com dados):**")
                        st.dataframe(historical_spread_series.tail(30))
                    else:
                        st.info(f"Nenhum período com Spread (%) acima de 1% encontrado para {ticker_to_analyze} no histórico disponível.")
                else:
                    st.warning(f"Não foi possível calcular o Spread (%) histórico para {ticker_to_analyze}. Verifique os dados brutos.")
            else:
                st.warning(f"Dados históricos brutos não disponíveis ou vazios para {ticker_to_analyze}.")
        else:
            st.warning(f"Dados históricos para {ticker_to_analyze} não disponíveis para análise do Spread (%).")
    elif analyze_spread_cycle and len(sel) != 1:
        st.info("Por favor, selecione exatamente um Ticker para analisar o ciclo do Spread (%).")

    # --- Nova seção de Depuração do Dividend Yield ---
    st.subheader(f"🔍 Depuração do Dividend Yield (DY % - {asset_type_display_name})")
    debug_dy_calc = st.checkbox(f"Exibir detalhes do cálculo do DY (%) ({asset_type_display_name}) (selecione apenas um Ticker)", key=f"debug_dy_{asset_type_display_name}")

    if debug_dy_calc and len(sel) == 1:
        ticker_to_debug_dy = sel[0]
        selected_row_original = df[df["Ticker"] == ticker_to_debug_dy]

        if not selected_row_original.empty and "Raw_Hist_Data" in selected_row_original.columns:
            hist_data = selected_row_original["Raw_Hist_Data"].iloc[0]
            current_price = selected_row_original["Cotação atual"].iloc[0]

            if hist_data is not None and not hist_data.empty and 'Dividends' in hist_data.columns and pd.notnull(current_price) and current_price != 0:
                most_recent_data_date = hist_data.index.max()
                start_date_12_months_ago = most_recent_data_date - timedelta(days=365)

                recent_dividends = hist_data.loc[hist_data.index >= start_date_12_months_ago, 'Dividends']
                total_dividends_last_12_months = recent_dividends.sum()

                st.write(f"**Detalhes do cálculo do DY (%) para:** {ticker_to_debug_dy}")
                st.write(f"**Cotação Atual:** {current_price:.2f}")
                st.write(f"**Data de Início para Dividendos (últimos 12 meses):** {start_date_12_months_ago.strftime('%Y-%m-%d')}")
                st.write(f"**Data Final para Dividendos (últimos 12 meses):** {most_recent_data_date.strftime('%Y-%m-%d')}")
                st.write(f"**Dividendos identificados nos últimos 12 meses:**")

                if not recent_dividends.empty:
                    st.dataframe(recent_dividends[recent_dividends > 0].reset_index().rename(columns={'index': 'Data', 'Dividends': 'Valor Dividendo'}))
                    st.write(f"**Soma total dos Dividendos nos últimos 12 meses:** {total_dividends_last_12_months:.2f}")
                    calculated_dy = (total_dividends_last_12_months / current_price) * 100 if current_price > 0 else 0.0
                    st.write(f"**DY (%) Calculado (Soma / Cotação Atual):** {calculated_dy:.2f}%")
                    st.info("Se houver uma diferença significativa, compare os dividendos listados acima com os proventos reportados por outras fontes para este ticker.")
                else:
                    st.info(f"Nenhum dividendo encontrado para {ticker_to_debug_dy} nos últimos 12 meses. DY calculado: 0.00%.")
            else:
                st.warning(f"Dados históricos (incluindo dividendos) ou cotação atual não disponíveis/válidos para {ticker_to_debug_dy} para depuração do DY.")
        else:
            st.warning(f"Dados históricos para {ticker_to_debug_dy} não disponíveis para depuração do DY.")
    elif debug_dy_calc and len(sel) != 1:
        st.info("Por favor, selecione exatamente um Ticker para exibir os detalhes do cálculo do DY (%).")

    # --- NOVO: Seção de Gráficos Interativos ---
    st.subheader(f"📈 Gráficos Interativos ({asset_type_display_name})")
    plot_interactive_chart = st.checkbox(f"Gerar Gráfico Interativo para {asset_type_display_name} (selecione apenas um Ticker)", key=f"plot_chart_{asset_type_display_name}")

    if plot_interactive_chart and len(sel) == 1:
        ticker_for_chart = sel[0]
        selected_row_original = df[df["Ticker"] == ticker_for_chart].iloc[0]

        hist_data_for_chart = selected_row_original["Raw_Hist_Data"]
        
        # Coleta os níveis de SR para passar para a função de plotagem
        sr_levels_dict = {
            'S3': selected_row_original.get('S3'),
            'S2': selected_row_original.get('S2'),
            'S1': selected_row_original.get('S1'),
            'P': selected_row_original.get('P'),
            'R1': selected_row_original.get('R1'),
            'R2': selected_row_original.get('R2'),
            'R3': selected_row_original.get('R3'),
        }

        # Coleta os dados de dividendos para passar para a função de plotagem
        events_data = None
        if asset_type_display_name in ["Ações", "FIIs", "BDRs"]:
            try:
                yf_ticker_obj = yf.Ticker(selected_row_original["Ticker_YF"])
                events_data = yf_ticker_obj.actions
            except Exception as e:
                st.warning(f"Não foi possível buscar dados de eventos para {ticker_for_chart}: {e}")

        visualize_price_data(hist_data_for_chart, ticker_for_chart, sr_levels_dict, events_data)
    elif plot_interactive_chart and len(sel) != 1:
        st.info("Por favor, selecione exatamente um Ticker para gerar o gráfico interativo.")


# --- Função display_indices_tab (MODIFICADA) ---
def display_indices_tab():
    """
    Exibe a aba de índices com o preço e variação do Ibovespa, Ouro, Petróleo, Dólar e outros índices globais.
    **Adicionada Dívida Pública (custo) do Brasil.**
    """
    st.header("📈 Cotações de Índices e Commodities Relevantes")

    # Tickers do yfinance
    # Ações/Mercados
    ibov_ticker = "^BVSP"       # Ibovespa
    sp500_ticker = "^GSPC"      # S&P 500
    nasdaq_ticker = "^IXIC"     # Nasdaq Composite
    dow_jones_ticker = "^DJI"   # Dow Jones Industrial Average
    euro_stoxx_ticker = "^STOXX50E" # Euro Stoxx 50
    nikkei_ticker = "^N225"     # Nikkei 225

    # Commodities
    gold_ticker = "GC=F"       # Futuro do Ouro em USD
    oil_ticker = "CL=F"        # Futuro do Petróleo WTI em USD
    silver_ticker = "SI=F"     # Futuro da Prata em USD
    natural_gas_ticker = "NG=F" # Futuro do Gás Natural em USD
    copper_ticker = "HG=F"     # Futuro do Cobre em USD
    coffee_ticker = "KC=F"     # Futuro do Café em USD
    wheat_ticker = "ZW=F"      # Futuro do Trigo em USD
    soybean_ticker = "ZS=F"    # Futuro da Soja em USD


    # Câmbio
    brl_usd_ticker = "BRL=X"    # Dólar Comercial (USD/BRL)

    # NOVO: Dívida Pública (como proxy: rendimento do título de 10 anos do Brasil)
    brazil_bond_yield_ticker = "BR10YT=X" # Rendimento do Título do Tesouro de 10 anos do Brasil

    # Busca os dados para cada ticker
    ibov_price, ibov_var = get_index_data(ibov_ticker)
    sp500_price, sp500_var = get_index_data(sp500_ticker)
    nasdaq_price, nasdaq_var = get_index_data(nasdaq_ticker)
    dow_jones_price, dow_jones_var = get_index_data(dow_jones_ticker)
    euro_stoxx_price, euro_stoxx_var = get_index_data(euro_stoxx_ticker)
    nikkei_price, nikkei_var = get_index_data(nikkei_ticker)

    gold_price, gold_var = get_index_data(gold_ticker)
    oil_price, oil_var = get_index_data(oil_ticker)
    silver_price, silver_var = get_index_data(silver_ticker)
    natural_gas_price, natural_gas_var = get_index_data(natural_gas_ticker)
    copper_price, copper_var = get_index_data(copper_ticker)
    coffee_price, coffee_var = get_index_data(coffee_ticker)
    wheat_price, wheat_var = get_index_data(wheat_ticker)
    soybean_price, soybean_var = get_index_data(soybean_ticker)

    brl_usd_price, brl_usd_var = get_index_data(brl_usd_ticker)

    # NOVO: Busca dados da Dívida Pública (rendimento)
    brazil_bond_yield_price, brazil_bond_yield_var = get_index_data(brazil_bond_yield_ticker)

    data = {
        "Ativo/Índice": [
            "Ibovespa (B3)",
            "S&P 500 (EUA)",
            "Nasdaq (EUA)",
            "Dow Jones (EUA)",
            "Euro Stoxx 50 (UE)",
            "Nikkei 225 (Japão)",
            "Ouro (USD)",
            "Prata (USD)",
            "Petróleo WTI (USD)",
            "Gás Natural (USD)",
            "Cobre (USD)",
            "Café (USD)",
            "Trigo (USD)",
            "Soja (USD)",
            "Dólar Comercial (USD/BRL)",
            "Dívida Pública (Rendimento 10a - Brasil)" # NOVO ATIVO
        ],
        "Cotação Atual": [
            ibov_price, sp500_price, nasdaq_price, dow_jones_price, euro_stoxx_price, nikkei_price,
            gold_price, silver_price, oil_price, natural_gas_price, copper_price, coffee_price,
            wheat_price, soybean_price, brl_usd_price,
            brazil_bond_yield_price # NOVO: Preço da Dívida Pública (Rendimento)
        ],
        "Variação (%)": [
            ibov_var, sp500_var, nasdaq_var, dow_jones_var, euro_stoxx_var, nikkei_var,
            gold_var, silver_var, oil_var, natural_gas_var, copper_var, coffee_var,
            wheat_var, soybean_var, brl_usd_var,
            brazil_bond_yield_var # NOVO: Variação da Dívida Pública (Rendimento)
        ]
    }
    df_indices = pd.DataFrame(data)

    # Estilização para a variação (verde para alta, vermelho para baixa)
    def highlight_variation(val):
        if pd.notnull(val):
            if val >= 0:
                return 'color: green; font-weight: bold'
            else:
                return 'color: red; font-weight: bold'
        return ''

    # Formatação dos números
    fmt = {
        "Cotação Atual": "{:.2f}",
        "Variação (%)": "{:.2f}%"
    }

    styled_df_indices = df_indices.style.format(fmt).applymap(
        highlight_variation, subset=pd.IndexSlice[:, ['Variação (%)']]
    )

    if not df_indices.empty:
        st.dataframe(styled_df_indices, hide_index=True, use_container_width=True)
    else:
        st.info("Não foi possível carregar os dados de todos os índices. Por favor, tente novamente mais tarde.")


# --- Função Principal da Aplicação Streamlit (MODIFICADA) ---

def main():
    """
    Função principal que organiza a interface do Streamlit.
    """
    try:
        # Informações de fuso horário na barra lateral (mantido para debug)
        st.sidebar.subheader("Informações de Fuso Horário (Debug)")
        tz_sp = pytz.timezone('America/Sao_Paulo')
        st.sidebar.write(f"Fuso horário do sistema (Render): {dt.now().astimezone().tzinfo}")
        st.sidebar.write(f"Data/hora UTC atual no Render: {dt.utcnow()}")
        st.sidebar.write(f"Data/hora local (do servidor) no Render: {dt.now()}")
        st.sidebar.write(f"Data/hora em São Paulo (no Render): {dt.now(tz_sp)}")
        st.sidebar.write("---")

        # NOVO: Controles de pesos para o Score na barra lateral (MODIFICADA)
        st.sidebar.subheader("Pesos para o Score de Atratividade")
        weights = {
            'volatility': st.sidebar.slider("Peso Volatilidade", 0.0, 2.0, 1.0, 0.1, key="weight_volatility"),
            'moving_averages': st.sidebar.slider("Peso Médias Móveis", 0.0, 2.0, 1.0, 0.1, key="weight_ma"),
            'rsi': st.sidebar.slider("Peso RSI", 0.0, 2.0, 1.0, 0.1, key="weight_rsi"),
            'bollinger_bands': st.sidebar.slider("Peso Bandas de Bollinger", 0.0, 2.0, 1.0, 0.1, key="weight_bb"),
            'volume': st.sidebar.slider("Peso Volume", 0.0, 2.0, 1.0, 0.1, key="weight_volume"),
            'cycles': st.sidebar.slider("Peso Ciclos", 0.0, 2.0, 1.0, 0.1, key="weight_cycles"),
            'spread_bonus': st.sidebar.slider("Peso Bônus Spread", 0.0, 2.0, 1.0, 0.1, key="weight_spread_bonus"),
            'target_proximity': st.sidebar.slider("Peso Proximidade Alvo", 0.0, 2.0, 1.0, 0.1, key="weight_target_proximity"),
            'growth_consistency': st.sidebar.slider("Peso Crescimento Consecutivo", 0.0, 2.0, 1.0, 0.1, key="weight_growth_consistency"),
            'momentum': st.sidebar.slider("Peso Momentum", 0.0, 2.0, 1.0, 0.1, key="weight_momentum"),
            'acceleration': st.sidebar.slider("Peso Aceleração", 0.0, 2.0, 1.0, 0.1, key="weight_acceleration"), # ADIÇÃO: Slider para peso da aceleração
        }
        st.sidebar.write("---")

        # Cria as abas superiores para navegação
        tab_stocks, tab_crypto, tab_etfs, tab_fiis, tab_bdrs, tab_indices = st.tabs(["Ações (B3)", "Criptomoedas", "ETFs", "FIIs", "BDRs", "Índices"])

        # Bloco para Ações
        with tab_stocks:
            st.header(f"Análise de Ações da B3 (Aba '{SHEET_NAME_STOCKS}')")
            process_and_display_data(SHEET_NAME_STOCKS, "Ações", weights)

        # Bloco para Criptomoedas
        with tab_crypto:
            st.header(f"Análise de Criptomoedas (Aba '{SHEET_NAME_CRYPTO}')")
            process_and_display_data(SHEET_NAME_CRYPTO, "Cripto", weights)

        # Bloco para ETFs
        with tab_etfs:
            st.header(f"Análise de ETFs (Aba '{SHEET_NAME_ETFS}')")
            process_and_display_data(SHEET_NAME_ETFS, "ETFs", weights)

        # Bloco para FIIs
        with tab_fiis:
            st.header(f"Análise de FIIs (Aba '{SHEET_NAME_FIIS}')")
            process_and_display_data(SHEET_NAME_FIIS, "FIIs", weights)

        # Bloco para BDRs
        with tab_bdrs:
            st.header(f"Análise de BDRs (Aba '{SHEET_NAME_BDRS}')")
            process_and_display_data(SHEET_NAME_BDRS, "BDRs", weights)

        # Bloco para Índices
        with tab_indices:
            display_indices_tab()

    except FileNotFoundError:
        st.error(f"❌ O arquivo '{EXCEL_PATH}' não foi encontrado. Certifique-se de que ele está no mesmo diretório da aplicação.")
    except Exception as e:
        st.error(f"❌ Erro ao processar dados: {e}. Por favor, verifique os logs para mais detalhes.")
        print(f"Erro detalhado: {e}")

if __name__ == "__main__":
    main()