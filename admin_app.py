# ÍNDICE (Completo e Final)
#
# 1. CONFIGURAÇÕES GLOBAIS E INICIALIZAÇÃO
# 2. FUNÇÕES DE AQUISIÇÃO E CARREGAMENTO DE DADOS
# 3. FUNÇÕES DE CÁLCULO E ANÁLISE DE INDICADORES
# 4. FUNÇÕES DE VISUALIZAÇÃO E ESTILIZAÇÃO
# 5. LÓGICA PRINCIPAL DA APLICAÇÃO STREAMLIT

# 1. CONFIGURAÇÕES GLOBAIS E INICIALIZAÇÃO
import os
import streamlit as st
import pandas as pd
import yfinance as yf
import unicodedata
import numpy as np
from datetime import date, timedelta, datetime as dt

st.set_page_config(page_title="📈 Análise de Preços Semanais - BOV2025", layout="wide")
st.title("📈 Análise de Preços Semanais - BOV2025")

EXCEL_PATH = "BOV2025_Analise_Completa_B.xlsx"
SHEET_NAME_STOCKS = "Streamlit"
SHEET_NAME_CRYPTO = "Criptos"
SHEET_NAME_ETFS = "ETF"
SHEET_NAME_FIIS = "FII"
SHEET_NAME_BDR = "BDR"

HIDDEN_FILES = ["hidden_cols.txt", "hidden_col.txt"]
DEFAULT_TICKERS_FILE = "default_tickers.txt"

SPREAD_ANALYSIS_DAYS = 30

# NOME DO ARQUIVO DE ESTADO PARA O MONITORAMENTO DE Status
STATUS_FILE = "Status_status.csv"


# 2. FUNÇÕES DE AQUISIÇÃO E CARREGAMENTO DE DADOS
@st.cache_data(ttl=300)
def carregar_planilha(path: str, aba: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=aba)
    df.columns = [unicodedata.normalize('NFC', str(col).strip()) for col in df.columns]
    df = df.dropna(axis=1, how="all")
    df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
    return df

def load_default_tickers(file_path: str, all_options: list) -> list:
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                loaded_tickers = [unicodedata.normalize('NFC', line.strip()) for line in f if line.strip()]
                return [t for t in loaded_tickers if t in all_options]
        except Exception as e:
            st.warning(f"Erro ao carregar tickers padrão: {e}")
    return []

@st.cache_data(ttl=60)
def get_price_var_min_max_last(ticker_yf: str):
    try:
        hist = yf.Ticker(ticker_yf).history(start=date.today() - timedelta(days=365), end=date.today() + timedelta(days=1))
        if hist.empty: return None, None, None, None, None, pd.DataFrame()

        close_today = round(hist["Close"].iloc[-1], 2) if len(hist) >= 1 else None
        var = round(((close_today - hist["Close"].iloc[-2]) / hist["Close"].iloc[-2]) * 100, 2) if len(hist) >= 2 and hist["Close"].iloc[-2] != 0 else None

        sextas = hist[hist.index.weekday == 4]
        min_sexta = round(sextas["Close"].min(), 2) if not sextas.empty else None
        max_sexta = round(sextas["Close"].max(), 2) if not sextas.empty else None
        fechamento_mais_recente = round(sextas["Close"].iloc[-1], 2) if not sextas.empty else None

        return close_today, var, min_sexta, max_sexta, fechamento_mais_recente, hist
    except Exception:
        return None, None, None, None, None, pd.DataFrame()

@st.cache_data(ttl=60)
def get_index_data(ticker_yf: str):
    try:
        hist = yf.Ticker(ticker_yf).history(period="2d")
        if hist.empty: return None, None
        close_today = round(hist["Close"].iloc[-1], 2)
        var = round(((close_today - hist["Close"].iloc[-2]) / hist["Close"].iloc[-2]) * 100, 2) if len(hist) >= 2 and hist["Close"].iloc[-2] != 0 else None
        return close_today, var
    except Exception as e:
        print(f"Erro ao buscar dados para o índice {ticker_yf}: {e}")
        return None, None

def carregar_status_anterior(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        return pd.DataFrame(columns=["Ticker", "Status_Anterior"])
    try:
        df_anterior = pd.read_csv(file_path)
        df_anterior = df_anterior.rename(columns={"Status": "Status_Anterior"})
        return df_anterior
    except Exception as e:
        st.warning(f"Não foi possível ler o arquivo de status anterior: {e}")
        return pd.DataFrame(columns=["Ticker", "Status_Anterior"])

def salvar_status_atual(df: pd.DataFrame, file_path: str):
    if "Ticker" in df.columns and "Status" in df.columns:
        try:
            df_to_save = df[["Ticker", "Status"]].copy()
            df_to_save.to_csv(file_path, index=False)
        except Exception as e:
            st.error(f"Erro ao salvar o arquivo de status: {e}")


# 3. FUNÇÕES DE CÁLCULO E ANÁLISE DE INDICADORES
def prever_alvo(row, last_cols, last_dates, next_friday):
    ys = [row[c] for c in last_cols]
    valid_data = [(last_dates[i].toordinal(), ys[i]) for i, y in enumerate(ys) if pd.notnull(y)]
    if len(valid_data) < 2: return None
    
    xs, ys_valid = zip(*valid_data)
    m, b = np.polyfit(xs, ys_valid, 1)
    return round(m * next_friday.toordinal() + b, 2)

def calcular_sr(row):
    H, L, C = row.get("Máxima sexta desde jun/24"), row.get("Mínima sexta desde jun/24"), row.get("Fechamento mais recente")
    if pd.notnull(H) and pd.notnull(L) and pd.notnull(C):
        P = (H + L + C) / 3
        return pd.Series([round(L - 2*(H-P), 2), round(P-(H-L), 2), round(2*P-H, 2), round(P, 2), round(2*P-L, 2), round(P+(H-L), 2), round(H+2*(P-L), 2)])
    return pd.Series([None]*7)

def encontrar_valores_proximos(row):
    preco = row.get("Cotação atual")
    niveis = sorted([v for k, v in row.items() if k in ["S3","S2","S1","P","R1","R2","R3"] and pd.notnull(v)])
    abaixo = max([v for v in niveis if v <= preco], default=None)
    acima = min([v for v in niveis if v > preco], default=None)
    return pd.Series([abaixo, acima])

def encontrar_var_faixa(row, k_values_list):
    var = row.get("Var")
    arr = sorted([v for v in k_values_list if pd.notnull(v)])
    if pd.notnull(var) and arr:
        abaixo = max([v for v in arr if v <= var], default=None)
        acima = min([v for v in arr if v > var], default=None)
        return pd.Series([abaixo, acima])
    return pd.Series([None, None])

def get_spread_analysis(full_hist_data: pd.DataFrame, days: int):
    if full_hist_data is None or full_hist_data.empty or days <= 0:
        return None, None
    
    try:
        start_date = full_hist_data.index.max() - timedelta(days=days)
        recent_hist = full_hist_data.loc[full_hist_data.index >= start_date].copy()

        historical_spreads = []
        full_hist_data.index = pd.to_datetime(full_hist_data.index)

        for i in range(len(recent_hist)):
            current_date = recent_hist.index[i]
            daily_hist = full_hist_data.loc[full_hist_data.index <= current_date]
            
            sextas = daily_hist[daily_hist.index.weekday == 4]
            H = round(sextas["Close"].max(), 2) if not sextas.empty else None
            L = round(sextas["Close"].min(), 2) if not sextas.empty else None
            C = round(sextas["Close"].iloc[-1], 2) if not sextas.empty else None
            
            Preco = round(daily_hist["Close"].iloc[-1], 2)
            Var = round(((Preco - daily_hist["Close"].iloc[-2]) / daily_hist["Close"].iloc[-2]) * 100, 2) if len(daily_hist) >= 2 and daily_hist["Close"].iloc[-2] != 0 else None
            
            temp_row = pd.Series({"Var": Var})

            if pd.notnull(H):
                P = (H + L + C) / 3
                sr_levels = [L - 2 * (H - P), P - (H - L), 2 * P - H, P, 2 * P - L, P + (H - L), H + 2 * (P - L)]
                nivel_abaixo = max((v for v in sr_levels if v <= Preco), default=None)
                nivel_acima = min((v for v in sr_levels if v > Preco), default=None)
            else:
                nivel_abaixo, nivel_acima = None, None

            amplitude = round(((nivel_acima / nivel_abaixo - 1) * 100), 2) if pd.notnull(nivel_abaixo) and nivel_abaixo != 0 and pd.notnull(nivel_acima) else None
            k_values = [round(amplitude / k, 2) if amplitude else None for k in [-2, -3, -5, -9, -17, -33, -65, 65, 33, 17, 9, 5, 3, 2]]
            var_faixa = encontrar_var_faixa(temp_row, k_values)
            
            spread_calculado = round(var_faixa[1] - var_faixa[0], 2) if pd.notnull(var_faixa[0]) and pd.notnull(var_faixa[1]) else None
            historical_spreads.append(spread_calculado)

        valid_spreads = pd.Series(historical_spreads).dropna()
        if valid_spreads.empty:
            return None, None
        
        return round(valid_spreads.min(), 2), round(valid_spreads.max(), 2)
        
    except Exception:
        return None, None

def analisar_spread(row):
    var, spread = row.get("Var"), row.get("Spread (%)")
    if pd.notnull(var) and pd.notnull(spread):
        if var > 0: return "Monitorando/ Bom" if spread > 1 else "Neutra"
        elif var < 0: return "Ótimo" if spread > 1 else "Esperar"
    return None

def calcular_margem(row, min_col_name, max_col_name):
    spread_atual = row.get('Spread (%)')
    spread_min = row.get(min_col_name)
    spread_max = row.get(max_col_name)
    
    if pd.notnull(spread_atual) and pd.notnull(spread_min) and pd.notnull(spread_max):
        intervalo = spread_max - spread_min
        if intervalo <= 0:
            return 100.0 if spread_atual <= spread_min else 0.0
        
        margem = ((spread_max - spread_atual) / intervalo) * 100
        return round(max(0, min(100, margem)), 2)
        
    return np.nan

def calcular_Status(row):
    var = row.get("Var")
    margem = row.get("Margem")
    if pd.isna(var) or pd.isna(margem): return None

    if margem > 90: return "Muito Baixa/ Atrativa"
    elif margem > 80: return "Baixa"
    elif margem > 50: return "Reduzida"
    elif margem > 20: return "Moderada"
    elif margem > 10: return "Alta/ Atenção"
    else: return "Muito Alta/ Evitar"

def calcular_cotacao_por_percentual(row, nome_coluna_percentual):
    cotacao = row.get("Cotação atual")
    percentual = row.get(nome_coluna_percentual)

    if pd.isna(cotacao) or pd.isna(percentual):
        return None

    preco_calculado = cotacao * (1 + (percentual / 100))
    return round(preco_calculado, 2)

def debug_spread_calculation(ticker_yf: str, days: int):
    st.write(f"--- \n### 🕵️‍♂️ Depuração do Spread para: **{ticker_yf}** nos últimos **{days}** dias")
    try:
        hist_data = yf.Ticker(ticker_yf).history(start=date.today() - timedelta(days=365), end=date.today() + timedelta(days=1))
        if hist_data.empty:
            st.error("Não foi possível obter dados históricos para o ticker.")
            return
    except Exception as e:
        st.error(f"Erro ao buscar dados do yfinance: {e}")
        return

    start_date = hist_data.index.max() - timedelta(days=days)
    recent_hist = hist_data.loc[hist_data.index >= start_date].copy()
    st.info(f"Analisando dados de {recent_hist.index.min().strftime('%Y-%m-%d')} até {recent_hist.index.max().strftime('%Y-%m-%d')}")

    debug_results = []
    hist_data.index = pd.to_datetime(hist_data.index)
    for i in range(len(recent_hist)):
        current_date = recent_hist.index[i]
        daily_hist = hist_data.loc[hist_data.index <= current_date]
        sextas = daily_hist[daily_hist.index.weekday == 4]
        H = round(sextas["Close"].max(), 2) if not sextas.empty else None
        L = round(sextas["Close"].min(), 2) if not sextas.empty else None
        C = round(sextas["Close"].iloc[-1], 2) if not sextas.empty else None
        Preco = round(daily_hist["Close"].iloc[-1], 2)
        Var = round(((Preco - daily_hist["Close"].iloc[-2]) / daily_hist["Close"].iloc[-2]) * 100, 2) if len(daily_hist) >= 2 and daily_hist["Close"].iloc[-2] != 0 else None
        temp_row = pd.Series({"Var": Var})
        if pd.notnull(H):
            P = (H + L + C) / 3
            sr_levels = [L - 2 * (H - P), P - (H - L), 2 * P - H, P, 2 * P - L, P + (H - L), H + 2 * (P - L)]
            nivel_abaixo = max((v for v in sr_levels if v <= Preco), default=None)
            nivel_acima = min((v for v in sr_levels if v > Preco), default=None)
        else:
            nivel_abaixo, nivel_acima = None, None
        amplitude = round(((nivel_acima / nivel_abaixo - 1) * 100), 2) if pd.notnull(nivel_abaixo) and nivel_abaixo != 0 and pd.notnull(nivel_acima) else None
        k_values = [round(amplitude / k, 2) if amplitude else None for k in [-2, -3, -5, -9, -17, -33, -65, 65, 33, 17, 9, 5, 3, 2]]
        var_faixa = encontrar_var_faixa(temp_row, k_values)
        spread_calculado = round(var_faixa[1] - var_faixa[0], 2) if pd.notnull(var_faixa[0]) and pd.notnull(var_faixa[1]) else None
        debug_results.append({
            "Data": current_date.strftime('%Y-%m-%d'),"Preço Fech.": Preco,"Var % Diária": Var,"H (Sexta) Usado": H,
            "L (Sexta) Usado": L,"Amplitude %": amplitude,"Var Faixa (Abaixo)": var_faixa[0],
            "Var Faixa (Acima)": var_faixa[1],"Spread_Calculado": spread_calculado
        })
    df_debug = pd.DataFrame(debug_results)
    if not df_debug.empty:
        st.dataframe(df_debug.style.format({
            "Preço Fech.": "{:.2f}","Var % Diária": "{:.2f}%","H (Sexta) Usado": "{:.2f}","L (Sexta) Usado": "{:.2f}",
            "Amplitude %": "{:.2f}%","Var Faixa (Abaixo)": "{:.2f}%","Var Faixa (Acima)": "{:.2f}%","Spread_Calculado": "{:.2f}%"
        }, na_rep="-"), use_container_width=True)
        valid_spreads = df_debug["Spread_Calculado"].dropna()
        if not valid_spreads.empty:
            min_spread, max_spread = valid_spreads.min(), valid_spreads.max()
            st.success(f"📈 **Spread Mínimo Calculado:** `{min_spread:.2f}%`")
            st.success(f"📉 **Spread Máximo Calculado:** `{max_spread:.2f}%`")
        else:
            st.warning("Nenhum valor de spread válido foi calculado no período.")

# 4. FUNÇÕES DE VISUALIZAÇÃO E ESTILIZAÇÃO
def highlight_positive_negative(val):
    if pd.isna(val) or not isinstance(val, (int, float)): return ''
    color = 'green' if val > 0 else 'red' if val < 0 else 'black'
    return f'color: {color};'

def highlight_margem(val):
    if pd.isna(val) or not isinstance(val, (int, float)):
        return ''
    color = 'green' if val > 50 else 'red'
    return f'color: {color}; font-weight: bold;'

# >>>>>>>>>>>>>>>>>>>>>>>>> 1. NOVA FUNÇÃO DE ESTILO CRIADA AQUI <<<<<<<<<<<<<<<<<<<<<<<<<<<
def highlight_alvo(row):
    """
    Estiliza a coluna 'Alvo' comparando com a 'Cotação atual'.
    Alvo > Cotação = Verde (potencial de alta)
    Alvo < Cotação = Vermelho (potencial de baixa)
    """
    alvo = row['Alvo']
    cotacao = row['Cotação atual']
    style = '' # Estilo padrão (sem cor)
    
    if pd.notnull(alvo) and pd.notnull(cotacao):
        if alvo > cotacao:
            style = 'color: green; font-weight: bold;'
        elif alvo < cotacao:
            style = 'color: red; font-weight: bold;'
            
    # Retorna uma série com o estilo apenas para a coluna 'Alvo'
    styles = pd.Series('', index=row.index)
    styles['Alvo'] = style
    return styles

def highlight_colunas_comparadas(row, colunas_para_estilo):
    styles = [''] * len(colunas_para_estilo)
    for i in range(1, len(colunas_para_estilo)):
        atual, anterior = row[colunas_para_estilo[i]], row[colunas_para_estilo[i-1]]
        if pd.notnull(atual) and pd.notnull(anterior):
            styles[i] = 'color: green; font-weight: bold;' if atual >= anterior else 'color: red; font-weight: bold;'
    return styles

def highlight_analise_spread(val):
    color_map = {"Ótimo": "green", "Monitorando/ Bom": "blue", "Neutra": "black", "Esperar": "red"}
    return f'color: {color_map.get(val, "")}; font-weight: bold' if color_map.get(val) else ''

def highlight_Status(val):
    if pd.isna(val): return ''
    color_map = {
        "Muito Alta/ Evitar": "red", "Alta/ Atenção": "red",
        "Moderada": "orange", "Reduzida": "orange",
        "Baixa": "green", "Muito Baixa/ Atrativa": "green"
    }
    return f'color: {color_map.get(val, "black")}; font-weight: bold;'


# 5. LÓGICA PRINCIPAL DA APLICAÇÃO STREAMLIT
def process_and_display_data(sheet_name: str, asset_type_display_name: str):
    df_base = carregar_planilha(EXCEL_PATH, sheet_name)
    if df_base.empty or "Ticker" not in df_base.columns:
        st.info(f"Nenhum dado válido na aba '{sheet_name}'."); return

    all_tickers = df_base["Ticker"].unique().tolist()
    default_selected = load_default_tickers(DEFAULT_TICKERS_FILE, all_tickers)
    
    sel = st.multiselect(f"Filtrar por Ticker ({asset_type_display_name}):", options=all_tickers, default=default_selected, key=f"multiselect_{asset_type_display_name}")

    if not sel: df = df_base.copy()
    else: df = df_base[df_base["Ticker"].isin(sel)].copy()
    
    if df.empty: st.info("Nenhum ticker para exibir."); return

    df_anterior = carregar_status_anterior(STATUS_FILE)

    with st.spinner(f"Buscando e processando dados para {len(df)} ticker(s)..."):
        df["Ticker_YF"] = df["Ticker"].astype(str).str.strip()
        if asset_type_display_name in ["Ações", "ETFs", "FIIs", "BDRs"]:
            df["Ticker_YF"] = df["Ticker_YF"] + ".SA"

        df[["Cotação atual", "Var", "Mínima sexta desde jun/24", "Máxima sexta desde jun/24", "Fechamento mais recente", "Raw_Hist_Data"]] = df["Ticker_YF"].apply(lambda t: pd.Series(get_price_var_min_max_last(t)))
        
        df.dropna(subset=['Raw_Hist_Data', 'Cotação atual'], inplace=True)
        if df.empty:
            st.warning("Não foi possível obter dados online para os tickers selecionados."); return

        date_cols = [c for c in df.columns if isinstance(c, str) and c[:4].isdigit() and "-" in c]
        for c in date_cols: df[c] = pd.to_numeric(df[c], errors="coerce")
        today = date.today()
        next_friday = today + timedelta(days=(4 - today.weekday() + 7) % 7)
        if len(date_cols) >= 4:
            last_cols = sorted(date_cols)[-4:]
            last_dates = [pd.to_datetime(c).date() for c in last_cols]
            df['Alvo'] = df.apply(lambda row: prever_alvo(row, last_cols, last_dates, next_friday), axis=1)
        else:
            df['Alvo'] = None
        df[["S3","S2","S1","P","R1","R2","R3"]] = df.apply(calcular_sr, axis=1)
        df[["Nível abaixo","Nível acima"]] = df.apply(encontrar_valores_proximos, axis=1)
        k_div = [-2,-3,-5,-9,-17,-33,-65,65,33,17,9,5,3,2]; k_cols = [f"K ({k})" for k in k_div]
        df["Amplitude"] = df.apply(lambda r: round(((r["Nível acima"]/r["Nível abaixo"]-1)*100), 2) if pd.notnull(r["Nível abaixo"]) and r["Nível abaixo"]!=0 and pd.notnull(r["Nível acima"]) else None, axis=1)
        df[k_cols] = df["Amplitude"].apply(lambda amp: pd.Series([round(amp/k, 2) if pd.notnull(amp) else None for k in k_div]))
        df[["Var (abaixo)","Var (acima)"]] = df.apply(lambda row: encontrar_var_faixa(row, row[k_cols].values), axis=1)
        df['Cotação Abaixo'] = df.apply(lambda row: calcular_cotacao_por_percentual(row, 'Var (abaixo)'), axis=1)
        df['Cotação Acima'] = df.apply(lambda row: calcular_cotacao_por_percentual(row, 'Var (acima)'), axis=1)
        df["Spread (%)"] = df.apply(lambda r: round(r["Var (acima)"] - r["Var (abaixo)"], 2) if pd.notnull(r["Var (abaixo)"]) and pd.notnull(r["Var (acima)"]) else None, axis=1)
        df["Análise spread"] = df.apply(analisar_spread, axis=1)

        col_min = f'Spread Mínimo ({SPREAD_ANALYSIS_DAYS}D)'
        col_max = f'Spread Máximo ({SPREAD_ANALYSIS_DAYS}D)'
        
        spread_stats = df['Raw_Hist_Data'].apply(lambda x: pd.Series(get_spread_analysis(x, days=SPREAD_ANALYSIS_DAYS)))
        spread_stats.columns = [col_min, col_max]
        
        df[col_min] = spread_stats[col_min]
        df[col_max] = spread_stats[col_max]

        df['Margem'] = df.apply(lambda row: calcular_margem(row, col_min, col_max), axis=1)
        df[col_max] = df[[col_max, 'Spread (%)']].max(axis=1)
        df['Status'] = df.apply(calcular_Status, axis=1)

    STATUS_ORDER = {"Muito fraca": 0, "Fraca": 1, "Neutra": 2, "Moderada": 4, "Forte": 5, "Muito forte": 6}
    df = pd.merge(df, df_anterior, on="Ticker", how="left")
    df['Status_Atual_Num'] = df['Status'].map(STATUS_ORDER)
    df['Status_Anterior_Num'] = df['Status_Anterior'].map(STATUS_ORDER)
    upgrades = df[df['Status_Atual_Num'] > df['Status_Anterior_Num']].copy()
    if not upgrades.empty:
        with st.expander("🚀 **Alertas de Melhoria de Status**", expanded=True):
            for _, row in upgrades.iterrows():
                st.markdown(f"- **{row['Ticker']}**: Mudou de `{row['Status_Anterior']}` ➔ **`{row['Status']}`**")
    else:
        st.info("Nenhuma melhoria de status na coluna 'Status' desde a última atualização.")
    salvar_status_atual(df, STATUS_FILE)
    
    hidden_cols_raw = []
    for fname in HIDDEN_FILES:
        if os.path.exists(fname):
            try:
                with open(fname, "r", encoding="utf-8") as f:
                    hidden_cols_raw = [unicodedata.normalize('NFC', line.strip()) for line in f if line.strip()]
                break
            except Exception as e:
                st.warning(f"Erro ao ler o arquivo '{fname}': {e}")
                
    cols_to_drop = ["Raw_Hist_Data", "Ticker_YF", "Status_Anterior", "Status_Atual_Num", "Status_Anterior_Num"]
    
    for hidden_name in hidden_cols_raw:
        for col in df.columns:
            if hidden_name in str(col):
                cols_to_drop.append(col)

    cols_to_drop = list(set(cols_to_drop))
    display_df = df.drop(columns=cols_to_drop, errors="ignore")
    
    date_cols_in_df = sorted([c for c in display_df.columns if isinstance(c, str) and c[:4].isdigit() and "-" in c])
    ideal_order = ['Ticker', 'Var', 'Margem', 'Cotação Abaixo', 'Cotação atual', 'Cotação Acima','Alvo', 'Status'] + date_cols_in_df + ['Análise spread', 'Spread (%)', col_max, col_min]
    ordered_cols = [col for col in ideal_order if col in display_df.columns]
    remaining_cols = [col for col in display_df.columns if col not in ordered_cols]
    final_order = ordered_cols + remaining_cols
    display_df = display_df[final_order]
    
    price_comparison_cols = date_cols_in_df + ["Cotação atual"]
    fmt = {col: "{:.2f}" for col in display_df.select_dtypes(include=[np.number]).columns}
    for col in ['Var', 'Spread (%)', col_max, col_min, 'Margem', 'Amplitude']:
        if col in display_df.columns: fmt[col] = "{:.2f}%"
        
    styled = display_df.style.format(fmt, na_rep="-")
    cols_pos_neg = ['Var', 'Spread (%)', 'Amplitude']
    for col in cols_pos_neg:
        if col in display_df.columns:
            styled = styled.applymap(highlight_positive_negative, subset=[col])
    if 'Análise spread' in display_df.columns:
        styled = styled.applymap(highlight_analise_spread, subset=['Análise spread'])
    if 'Status' in display_df.columns:
        styled = styled.applymap(highlight_Status, subset=['Status'])
        
    if 'Margem' in display_df.columns:
        styled = styled.applymap(highlight_margem, subset=['Margem'])
        
    valid_price_cols = [col for col in price_comparison_cols if col in display_df.columns]
    if valid_price_cols:
        styled = styled.apply(highlight_colunas_comparadas, colunas_para_estilo=valid_price_cols, axis=1, subset=pd.IndexSlice[:, valid_price_cols])
    
    # >>>>>>>>>>>>>>>>>>>>>>>>> 2. APLICAÇÃO DO NOVO ESTILO <<<<<<<<<<<<<<<<<<<<<<<<<<<
    if 'Alvo' in display_df.columns:
        styled = styled.apply(highlight_alvo, axis=1)

    st.dataframe(styled, use_container_width=True)

def display_indices_tab():
    st.header("📈 Cotações de Índices e Commodities")
    indices = { "Ibovespa": "^BVSP", "S&P 500": "^GSPC", "Dólar (USD/BRL)": "BRL=X"}
    data = []
    for nome, ticker in indices.items():
        preco, var = get_index_data(ticker)
        data.append({"Índice": nome, "Cotação": preco, "Variação %": var})
    df_indices = pd.DataFrame(data)
    if not df_indices.empty:
        st.dataframe(df_indices.style.format({"Cotação":"{:.2f}", "Variação %":"{:.2f}%"}, na_rep="-").applymap(highlight_positive_negative, subset=["Variação %"]), hide_index=True, use_container_width=True)

def main():
    try:
        tab_stocks, tab_crypto, tab_etfs, tab_fiis, tab_bdrs, tab_indices, tab_debug = st.tabs([
            "Ações (B3)", "Criptomoedas", "ETFs", "FIIs", "BDRs", "Índices", "Debug 🕵️‍♂️"
        ])
        with tab_stocks:
            st.header(f"Análise de Ações da B3"); process_and_display_data(SHEET_NAME_STOCKS, "Ações")
        with tab_crypto:
            st.header(f"Análise de Criptomoedas"); process_and_display_data(SHEET_NAME_CRYPTO, "Cripto")
        with tab_etfs:
            st.header(f"Análise de ETFs"); process_and_display_data(SHEET_NAME_ETFS, "ETFs")
        with tab_fiis:
            st.header(f"Análise de FIIs"); process_and_display_data(SHEET_NAME_FIIS, "FIIs")
        with tab_bdrs:
            st.header(f"Análise de BDRs"); process_and_display_data(SHEET_NAME_BDR, "BDRs")
        with tab_indices:
            display_indices_tab()
        with tab_debug:
            st.header("Ferramenta de Depuração de Spread")
            ticker_para_debugar = st.text_input(
                "Digite o ticker para depurar (ex: PETR4.SA, MGLU3.SA, BTC-USD):", "XRP-USD"
            )
            if st.button("Executar Depuração"):
                if ticker_para_debugar:
                    debug_spread_calculation(ticker_para_debugar.strip().upper(), SPREAD_ANALYSIS_DAYS)
                else:
                    st.warning("Por favor, insira um ticker.")
    except FileNotFoundError:
        st.error(f"❌ O arquivo '{EXCEL_PATH}' não foi encontrado.")
    except Exception as e:
        st.error(f"❌ Um erro inesperado ocorreu: {e}."); import traceback; traceback.print_exc()

if __name__ == "__main__":
    main()