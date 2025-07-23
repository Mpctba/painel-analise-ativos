import os
import streamlit as st
import pandas as pd
import yfinance as yf
import unicodedata
import numpy as np
from datetime import date, timedelta, datetime as dt
import pytz # Importação adicionada para lidar com fusos horários

# --- Configurações Iniciais da Aplicação ---
st.set_page_config(page_title="📈 Análise de Preços Semanais - BOV2025", layout="wide")
st.title("📈 Análise de Preços Semanais - BOV2025")

EXCEL_PATH = "BOV2025_Analise_Completa_B.xlsx"
SHEET_NAME_STOCKS = "Streamlit" # Aba para ações da B3
SHEET_NAME_CRYPTO = "Criptos" # Aba para criptomoedas
SHEET_NAME_ETFS = "ETF" # Aba para ETFs
SHEET_NAME_FIIS = "FII" # Nova aba para FIIs

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
    niveis = [row.get(k) for k in ["S3","S2","S1","P","R1","R2","R3"] if pd.notnull(row.get(k))]
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
        return pd.Series([aba,ac])
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

# --- FUNÇÃO calculate_consecutive_growth (SEM ALTERAÇÕES, pois já conta crescimento ESTITRO para "Nível") ---
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
        elif curr_val > prev_val: # AQUI A CONDIÇÃO É AINDA ESTRICTA, como solicitado para o "Nível" anterior
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


# --- Função Principal de Processamento e Exibição de Dados ---

def process_and_display_data(sheet_name: str, asset_type_display_name: str):
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
    # Adiciona .SA apenas para ações, ETFs e FIIs
    if asset_type_display_name in ["Ações", "ETFs", "FIIs"]:
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

    ocultar = [col for col in hidden_cols if col in df.columns] + ["Raw_Hist_Data"] if hidden_cols else ["Raw_Hist_Data"]
    # Garante que a nova coluna não seja ocultada por padrão
    if 'Nível' in ocultar:
        ocultar.remove('Nível')

    display_df = df.drop(columns=ocultar, errors="ignore")

    # Verifica se display_df está vazio após a remoção de colunas
    if display_df.empty:
        st.info(f"Nenhum dado para exibir na aba '{asset_type_display_name}' após a remoção de colunas. Por favor, verifique a planilha ou as colunas ocultas.")
        st.dataframe(pd.DataFrame()) # Exibe um DataFrame vazio
        return # Sai da função

    cols = list(display_df.columns)
    # Reordena as colunas para exibir "Cotação atual" após "Ticker_YF"
    if "Ticker_YF" in cols and "Cotação atual" in cols:
        cols.remove("Cotação atual"); i = cols.index("Ticker_YF"); cols.insert(i+1,"Cotação atual")
        # Inserir 'Nível' após 'Cotação atual'
        if 'Nível' in cols:
            cols.remove('Nível')
            idx_cotacao_atual = cols.index("Cotação atual")
            cols.insert(idx_cotacao_atual + 1, "Nível")
        display_df = display_df[cols]
    elif 'Nível' in cols:
        pass # A coluna já estará na exibição, não precisa de reordenação específica

    fmt = {col: "{:.2f}" for col in display_df.select_dtypes(include=[np.number]).columns}
    # Formata a nova coluna como inteiro, pois é um nível de 0 a 5
    if 'Nível' in fmt:
        fmt['Nível'] = "{:.0f}"

    display_df.columns = [str(c) for c in display_df.columns]
    date_cols_fmt = [c for c in display_df.columns if c[:4].isdigit() and "-" in c]
    # Garante que pegamos no máximo as últimas 5, mas lida graciosamente com menos de 5
    if len(date_cols_fmt) > 5:
        date_cols_fmt = sorted(date_cols_fmt)[-5:]
    else:
        date_cols_fmt = sorted(date_cols_fmt) # Ordena todas se houver menos de 5
    
    # Garante que colunas_para_estilo seja sempre definida, mesmo que vazia
    colunas_para_estilo = []
    if "Cotação atual" in display_df.columns:
        colunas_para_estilo = date_cols_fmt + ["Cotação atual"]
    else:
        colunas_para_estilo = date_cols_fmt

    # Aplica estilização somente se houver colunas para estilizar e o DataFrame não estiver vazio
    if not display_df.empty and colunas_para_estilo:
        styled = display_df.style.format(fmt)
        styled = styled.apply(lambda row: highlight_colunas_comparadas(row, colunas_para_estilo), axis=1, subset=colunas_para_estilo)
        st.dataframe(styled)
    else:
        # Se não houver colunas para estilizar ou o DataFrame estiver vazio, exibe sem estilização condicional
        st.dataframe(display_df.style.format(fmt))

    # Seção de Depuração de Dados de Sexta-feira
    st.subheader(f"🛠️ Histórico de Dados de Sexta-feira ({asset_type_display_name})")
    debug_friday_data = st.checkbox(f"Exibir dados brutos de Sexta-feira para depuração ({asset_type_display_name}) (selecione apenas um Ticker)", key=f"debug_friday_{asset_type_display_name}")

    if debug_friday_data and len(sel) == 1:
        ticker_to_debug_base = sel[0]
        selected_row = df[df["Ticker"] == ticker_to_debug_base]
        if not selected_row.empty and "Raw_Hist_Data" in selected_row.columns:
            hist_data = selected_row["Raw_Hist_Data"].iloc[0]
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

            valid_indices_debug = [i for i, y in enumerate(ys) if pd.notnull(y)]
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
        selected_row = df[df["Ticker"] == ticker_to_analyze]
        
        if not selected_row.empty and "Raw_Hist_Data" in selected_row.columns:
            hist_data = selected_row["Raw_Hist_Data"].iloc[0]
            
            if hist_data is not None and not hist_data.empty:
                st.write(f"Calculando Spread (%) histórico para {ticker_to_analyze}...")
                
                # Exibe um spinner enquanto o cálculo é feito
                with st.spinner("Isso pode levar alguns segundos para históricos longos..."):
                    historical_spread_series = calculate_historical_spread(hist_data.copy()) # Use uma cópia para evitar modificar o DataFrame em cache

                if not historical_spread_series.empty:
                    # Identifica períodos consecutivos onde o spread está acima de 1%
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

# --- Função display_indices_tab (mantida como a última versão completa) ---
def display_indices_tab():
    """
    Exibe a aba de índices com o preço e variação do Ibovespa, Ouro, Petróleo, Dólar e outros índices globais.
    """
    st.header("📈 Cotações de Índices e Commodities Relevantes")

    # Tickers do yfinance
    # Ações/Mercados
    ibov_ticker = "^BVSP"      # Ibovespa
    sp500_ticker = "^GSPC"     # S&P 500
    nasdaq_ticker = "^IXIC"    # Nasdaq Composite
    dow_jones_ticker = "^DJI"  # Dow Jones Industrial Average
    euro_stoxx_ticker = "^STOXX50E" # Euro Stoxx 50
    nikkei_ticker = "^N225"    # Nikkei 225

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
    brl_usd_ticker = "BRL=X"   # Dólar Comercial (USD/BRL)

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
            "Dólar Comercial (USD/BRL)"
        ],
        "Cotação Atual": [
            ibov_price, sp500_price, nasdaq_price, dow_jones_price, euro_stoxx_price, nikkei_price,
            gold_price, silver_price, oil_price, natural_gas_price, copper_price, coffee_price,
            wheat_price, soybean_price, brl_usd_price
        ],
        "Variação (%)": [
            ibov_var, sp500_var, nasdaq_var, dow_jones_var, euro_stoxx_var, nikkei_var,
            gold_var, silver_var, oil_var, natural_gas_var, copper_var, coffee_var,
            wheat_var, soybean_var, brl_usd_var
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
        st.dataframe(styled_df_indices, hide_index=True)
    else:
        st.info("Não foi possível carregar os dados de todos os índices. Por favor, tente novamente mais tarde.")


# --- Função Principal da Aplicação Streamlit ---

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

        # Cria as abas superiores para navegação (REMOVENDO TAB_NOTICIAS)
        tab_stocks, tab_crypto, tab_etfs, tab_fiis, tab_indices = st.tabs(["Ações (B3)", "Criptomoedas", "ETFs", "FIIs", "Índices"])

        # Bloco para Ações
        with tab_stocks:
            st.header(f"Análise de Ações da B3 (Aba '{SHEET_NAME_STOCKS}')")
            process_and_display_data(SHEET_NAME_STOCKS, "Ações")

        # Bloco para Criptomoedas
        with tab_crypto:
            st.header(f"Análise de Criptomoedas (Aba '{SHEET_NAME_CRYPTO}')")
            process_and_display_data(SHEET_NAME_CRYPTO, "Cripto")

        # Bloco para ETFs
        with tab_etfs:
            st.header(f"Análise de ETFs (Aba '{SHEET_NAME_ETFS}')")
            process_and_display_data(SHEET_NAME_ETFS, "ETFs")

        # Bloco para FIIs
        with tab_fiis:
            st.header(f"Análise de FIIs (Aba '{SHEET_NAME_FIIS}')")
            process_and_display_data(SHEET_NAME_FIIS, "FIIs")
            
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