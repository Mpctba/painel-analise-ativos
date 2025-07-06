# app.py

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
SHEET_NAME_CRYPTO = "Criptos" # Nova aba para criptomoedas

HIDDEN_FILES = ["hidden_cols.txt", "hidden_col.txt"]

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

# --- Funções de Cálculo e Análise ---
# (Sem alterações aqui, pois a lógica de cálculo é independente da moeda)

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

def encontrar_var_faixa(row, k_cols):
    """Encontra a faixa de variação (K) em que a variação atual se encaixa."""
    var = row.get("Var")
    arr = sorted([row.get(c) for c in k_cols if pd.notnull(row.get(c))])
    aba = max([v for v in arr if v<=var], default=None)
    ac = min([v for v in arr if v > var], default=None)
    return pd.Series([aba,ac])

def prever_alvo(row, last_cols, last_dates, next_friday):
    """Prevê o valor alvo usando regressão linear simples."""
    ys = [row[c] for c in last_cols]
    if any(pd.isnull(ys)):
        return None
    xs = [d.toordinal() for d in last_dates]
    m, b = np.polyfit(xs, ys, 1)
    return round(m * next_friday.toordinal() + b, 2)

def highlight_colunas_comparadas(row, colunas_para_estilo):
    """Aplica estilo de cor (verde/vermelho) às colunas de cotação com base na comparação com o valor anterior."""
    vals = row[colunas_para_estilo].values
    styles = [''] * len(vals)
    for i in range(1, len(vals)):
        ant = vals[i-1]
        atual = vals[i]
        if pd.notnull(ant) and pd.notnull(atual):
            if atual > ant:
                styles[i] = 'color: green; font-weight: bold'
            elif atual < ant:
                styles[i] = 'color: red; font-weight: bold'
    return styles

# --- Função Principal de Processamento e Exibição de Dados ---

def process_and_display_data(sheet_name: str, asset_type_display_name: str):
    """
    Função auxiliar para processar e exibir os dados para um tipo de ativo específico,
    reduzindo a duplicação de código.
    """
    df = carregar_planilha(EXCEL_PATH, sheet_name)

    hidden_cols_raw = []
    for fname in HIDDEN_FILES:
        if os.path.exists(fname):
            with open(fname, "r", encoding="utf-8") as f:
                hidden_cols_raw = [line.strip() for line in f if line.strip()]
            break
    hidden_cols = [unicodedata.normalize('NFC', h) for h in hidden_cols_raw]

    if "Ticker" not in df.columns:
        st.warning(f"A coluna 'Ticker' não foi encontrada na planilha '{sheet_name}'. Certifique-se de que a coluna existe e está nomeada corretamente.")
        return # Sai da função se não houver coluna Ticker

    # Lógica de formatação do Ticker para yFinance
    df["Ticker_YF"] = df["Ticker"].astype(str).str.strip()
    if asset_type_display_name == "Ações": # Adiciona .SA apenas para ações
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

    # --- REMOVIDO: Lógica de conversão para BRL se for criptomoeda foi removida aqui ---

    # --- Aplicação dos cálculos de Suporte/Resistência e outros indicadores ---
    df[["S3","S2","S1","P","R1","R2","R3"]] = df.apply(calcular_sr, axis=1)
    df[["Nível abaixo","Nível acima"]] = df.apply(encontrar_valores_proximos, axis=1)
    df.rename(columns={"Distância percentual": "Delta"}, inplace=True)
    df["Delta"] = df.apply(calcular_distancia_percentual, axis=1)
    df["Amplitude"] = df.apply(lambda r: round(((r.get("Nível acima")/r.get("Nível abaixo")-1)*100), 2) if pd.notnull(r.get("Nível abaixo")) and r.get("Nível abaixo")!=0 else None, axis=1)

    k_div = [-2,-3,-5,-9,-17,-33,-65,65,33,17,9,5,3,2]
    k_cols = [f"K ({k})" for k in k_div]
    df[k_cols] = df["Amplitude"].apply(lambda amp: pd.Series([round(amp/k, 2) if pd.notnull(amp) else None for k in k_div]))

    df[["Var (abaixo)","Var (acima)"]] = df.apply(lambda row: encontrar_var_faixa(row, k_cols), axis=1)
    df["Spread (%)"] = df.apply(lambda r: round(r.get("Var (acima)")-r.get("Var (abaixo)"), 2) if pd.notnull(r.get("Var (abaixo)")) and pd.notnull(r.get("Var (acima)")) else None, axis=1)

    date_cols = [c for c in df.columns if c[:4].isdigit() and "-" in c]
    for c in date_cols: df[c] = pd.to_numeric(df[c],errors="coerce")

    today = date.today()
    wd = today.weekday()
    offset = (4 - wd) % 7
    offset = offset if offset != 0 else 7
    next_friday = today + timedelta(days=offset)

    last_cols = date_cols[-4:]
    last_dates = []
    for col in last_cols:
        try:
            d = dt.fromisoformat(str(col))
        except ValueError:
            d = pd.to_datetime(col)
        last_dates.append(d.date())

    df['Alvo'] = df.apply(lambda row: prever_alvo(row, last_cols, last_dates, next_friday), axis=1)

    # Filtro de ticker e exibição do DataFrame
    opt = df["Ticker"].unique().tolist()
    sel = st.multiselect(f"Filtrar por Ticker ({asset_type_display_name}):", options=opt, default=[], key=f"multiselect_{asset_type_display_name}")
    if sel: df = df[df["Ticker"].isin(sel)]

    ocultar = [col for col in hidden_cols if col in df.columns] + ["Raw_Hist_Data"] if hidden_cols else ["Raw_Hist_Data"]
    display_df = df.drop(columns=ocultar, errors="ignore")

    cols = list(display_df.columns)
    if "Ticker_YF" in cols and "Cotação atual" in cols:
        cols.remove("Cotação atual"); i = cols.index("Ticker_YF"); cols.insert(i+1,"Cotação atual")
        display_df = display_df[cols]

    fmt = {col: "{:.2f}" for col in display_df.select_dtypes(include=[np.number]).columns}
    display_df.columns = [str(c) for c in display_df.columns]
    date_cols_fmt = [c for c in display_df.columns if c[:4].isdigit() and "-" in c]
    date_cols_fmt = sorted(date_cols_fmt)[-5:]
    colunas_para_estilo = date_cols_fmt + ["Cotação atual"] if "Cotação atual" in display_df.columns else date_cols_fmt

    styled = display_df.style.format(fmt)
    styled = styled.apply(lambda row: highlight_colunas_comparadas(row, colunas_para_estilo), axis=1, subset=colunas_para_estilo)

    st.dataframe(styled)

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

        # Cria as abas superiores para navegação
        tab_stocks, tab_crypto = st.tabs(["Ações (B3)", "Criptomoedas"])

        # Bloco para Ações
        with tab_stocks:
            st.header(f"Análise de Ações da B3 (Aba '{SHEET_NAME_STOCKS}')")
            process_and_display_data(SHEET_NAME_STOCKS, "Ações")

        # Bloco para Criptomoedas
        with tab_crypto:
            st.header(f"Análise de Criptomoedas (Aba '{SHEET_NAME_CRYPTO}')")
            process_and_display_data(SHEET_NAME_CRYPTO, "Cripto")

    except FileNotFoundError:
        st.error(f"❌ O arquivo '{EXCEL_PATH}' não foi encontrado. Certifique-se de que ele está no mesmo diretório da aplicação.")
    except Exception as e:
        st.error(f"❌ Erro ao processar dados: {e}. Por favor, verifique os logs para mais detalhes.")
        print(f"Erro detalhado: {e}")

if __name__ == "__main__":
    main()