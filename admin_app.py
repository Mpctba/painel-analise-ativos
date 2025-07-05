# app.py

import os
import streamlit as st
import pandas as pd
import yfinance as yf
import unicodedata
import numpy as np
from datetime import date, timedelta, datetime as dt
import pytz # Importação adicionada para lidar com fusos horários

st.set_page_config(page_title="📈 Análise de Preços Semanais - BOV2025", layout="wide")
st.title("📈 Análise de Preços Semanais - BOV2025")

EXCEL_PATH = "BOV2025_Analise_Completa_B.xlsx"
SHEET_NAME = "Streamlit"
HIDDEN_FILES = ["hidden_cols.txt", "hidden_col.txt"]

@st.cache_data(ttl=300)
def carregar_planilha(path: str, aba: str) -> pd.DataFrame:
    """
    Carrega os dados de uma planilha Excel e normaliza os nomes das colunas.
    Remove colunas vazias e não nomeadas.
    """
    df = pd.read_excel(path, sheet_name=aba)
    # Normaliza os nomes das colunas para remover acentos e espaços extras
    df.columns = [unicodedata.normalize('NFC', str(col).strip()) for col in df.columns]
    df = df.dropna(axis=1, how="all") # Remove colunas que são completamente vazias
    df = df.loc[:, ~df.columns.str.startswith("Unnamed:")] # Remove colunas "Unnamed"
    return df

@st.cache_data(ttl=3600)
def get_price_var_min_max_last(ticker_yf: str):
    """
    Busca dados de cotação usando yfinance, calcula variação,
    mínima/máxima de sextas-feiras e fechamento mais recente.
    Ajustado para lidar com fuso horário de São Paulo.
    """
    try:
        ticker_data = yf.Ticker(ticker_yf)

        # Define o fuso horário de São Paulo (para o mercado brasileiro)
        tz_sp = pytz.timezone('America/Sao_Paulo')

        # Obtém a data e hora atual no fuso horário de São Paulo
        now_sp = dt.now(tz_sp)
        # A data final para a busca do yfinance. Adicionamos 1 dia porque yfinance.history(end=X)
        # busca dados até X-1. Para incluir o dia 'hoje', precisamos passar 'hoje + 1 dia'.
        end_date_yf = now_sp.date() + timedelta(days=1)

        # Busca o histórico de preços. O 'start' é fixo, mas o 'end' garante que
        # estamos pegando os dados mais recentes até 'hoje' no fuso horário de SP.
        hist = ticker_data.history(start="2024-06-01", end=end_date_yf)

        if hist.empty:
            # Se não houver dados, retorna None para todas as variáveis
            return None, None, None, None, None

        # Calcula a cotação atual e a variação diária
        recent = hist.tail(2) # Pega os dois últimos dias de negociação
        if len(recent) >= 2:
            close_today = round(recent["Close"].iloc[-1], 2)
            close_yesterday = recent["Close"].iloc[-2]
            var = round(((close_today - close_yesterday) / close_yesterday) * 100, 2)
        elif len(recent) == 1:
            # Se houver apenas um dia (ex: primeiro dia de dados ou feriado no dia anterior)
            close_today = round(recent["Close"].iloc[-1], 2)
            var = None # Não há variação para calcular
        else:
            close_today = None
            var = None

        # Filtra os dados apenas para sextas-feiras
        # O yfinance retorna o índice com o dia da semana correto para o mercado local,
        # então não precisamos de conversão de fuso horário aqui, apenas garantimos
        # que os dados foram buscados até a data correta de SP.
        sextas = hist[hist.index.weekday == 4] # 4 representa sexta-feira (0=segunda, 6=domingo)
        min_sexta = round(sextas["Close"].min(), 2) if not sextas.empty else None
        max_sexta = round(sextas["Close"].max(), 2) if not sextas.empty else None
        fechamento_mais_recente = round(sextas["Close"].iloc[-1], 2) if not sextas.empty else None

        return close_today, var, min_sexta, max_sexta, fechamento_mais_recente
    except Exception as e:
        # Imprime o erro no console do Render para facilitar a depuração
        print(f"Erro ao buscar dados para {ticker_yf}: {e}")
        return None, None, None, None, None

def main():
    """
    Função principal da aplicação Streamlit.
    Carrega dados, calcula indicadores e exibe a tabela.
    """
    try:
        # Exibe informações de fuso horário para depuração no Render
        st.sidebar.subheader("Informações de Fuso Horário (Debug)")
        tz_sp = pytz.timezone('America/Sao_Paulo')
        st.sidebar.write(f"Fuso horário do sistema (Render): {dt.now().astimezone().tzinfo}")
        st.sidebar.write(f"Data/hora UTC atual no Render: {dt.utcnow()}")
        st.sidebar.write(f"Data/hora local (do servidor) no Render: {dt.now()}")
        st.sidebar.write(f"Data/hora em São Paulo (no Render): {dt.now(tz_sp)}")
        st.sidebar.write("---")


        df = carregar_planilha(EXCEL_PATH, SHEET_NAME)

        # Carrega as colunas ocultas de arquivos de texto
        hidden_cols_raw = []
        for fname in HIDDEN_FILES:
            if os.path.exists(fname):
                with open(fname, "r", encoding="utf-8") as f:
                    hidden_cols_raw = [line.strip() for line in f if line.strip()]
                break

        hidden_cols = [unicodedata.normalize('NFC', h) for h in hidden_cols_raw]

        if "Ticker" not in df.columns:
            st.warning("A coluna 'Ticker' não foi encontrada na planilha.")
            st.stop()

        # Adiciona a extensão ".SA" para tickers brasileiros no yfinance
        df["Ticker_YF"] = df["Ticker"].astype(str).str.strip() + ".SA"

        # Aplica a função para obter os dados do yfinance
        df[[
            "Cotação atual",
            "Var",
            "Mínima sexta desde jun/24",
            "Máxima sexta desde jun/24",
            "Fechamento mais recente",
        ]] = df["Ticker_YF"].apply(lambda t: pd.Series(get_price_var_min_max_last(t)))

        def calcular_sr(row):
            """
            Calcula os pontos de suporte e resistência (SR) com base nos valores
            de máxima, mínima e fechamento mais recente.
            """
            H = row.get("Máxima sexta desde jun/24")
            L = row.get("Mínima sexta desde jun/24")
            C = row.get("Fechamento mais recente")
            if pd.notnull(H) and pd.notnull(L) and pd.notnull(C):
                P = (H + L + C) / 3
                return pd.Series([round(L - 2*(H-P), 2), round(P-(H-L), 2), round(2*P-H, 2), round(P, 2), round(2*P-L, 2), round(P+(H-L), 2), round(H+2*(P-L), 2)])
            return pd.Series([None]*7)

        df[["S3","S2","S1","P","R1","R2","R3"]] = df.apply(calcular_sr, axis=1)

        def encontrar_valores_proximos(row):
            """
            Encontra os níveis de suporte/resistência mais próximos (abaixo e acima)
            da cotação atual.
            """
            preco = row.get("Cotação atual")
            niveis = [row.get(k) for k in ["S3","S2","S1","P","R1","R2","R3"] if pd.notnull(row.get(k))]
            niveis.sort()
            abaixo = max([v for v in niveis if v<=preco], default=None)
            acima = min([v for v in niveis if v>preco], default=None)
            return pd.Series([abaixo,acima])

        df[["Nível abaixo","Nível acima"]] = df.apply(encontrar_valores_proximos, axis=1)

        def calcular_distancia_percentual(row):
            """
            Calcula a menor distância percentual da cotação atual para o nível
            de suporte/resistência mais próximo.
            """
            preco = row.get("Cotação atual")
            abaixo = row.get("Nível abaixo")
            acima  = row.get("Nível acima")
            d1 = abs((preco-abaixo)/preco)*100 if pd.notnull(abaixo) and preco != 0 else None
            d2 = abs((acima-preco)/preco)*100 if pd.notnull(acima) and preco != 0 else None
            return round(min([d for d in [d1,d2] if d is not None], default=None), 2) if d1 is not None or d2 is not None else None

        df.rename(columns={"Distância percentual": "Delta"}, inplace=True)
        df["Delta"] = df.apply(calcular_distancia_percentual, axis=1)
        df["Amplitude"] = df.apply(lambda r: round(((r.get("Nível acima")/r.get("Nível abaixo")-1)*100), 2) if pd.notnull(r.get("Nível abaixo")) and r.get("Nível abaixo")!=0 else None, axis=1)

        k_div = [-2,-3,-5,-9,-17,-33,-65,65,33,17,9,5,3,2]
        k_cols = [f"K ({k})" for k in k_div]
        df[k_cols] = df["Amplitude"].apply(lambda amp: pd.Series([round(amp/k, 2) if pd.notnull(amp) else None for k in k_div]))

        def encontrar_var_faixa(row):
            """
            Encontra a faixa de variação (K) em que a variação atual se encaixa.
            """
            var = row.get("Var")
            arr = sorted([row.get(c) for c in k_cols if pd.notnull(row.get(c))])
            aba = max([v for v in arr if v<=var], default=None)
            ac  = min([v for v in arr if v > var], default=None)
            return pd.Series([aba,ac])

        df[["Var (abaixo)","Var (acima)"]] = df.apply(encontrar_var_faixa, axis=1)
        df["Spread (%)"] = df.apply(lambda r: round(r.get("Var (acima)")-r.get("Var (abaixo)"), 2) if pd.notnull(r.get("Var (abaixo)")) and pd.notnull(r.get("Var (acima)")) else None, axis=1)

        # Converte colunas de data para tipo numérico (se necessário)
        date_cols = [c for c in df.columns if c[:4].isdigit() and "-" in c]
        for c in date_cols: df[c] = pd.to_numeric(df[c],errors="coerce")

        # Calcula a próxima sexta-feira para projeção de alvo
        today = date.today()
        wd = today.weekday()
        offset = (4 - wd) % 7
        offset = offset if offset != 0 else 7 # Se já for sexta, pega a próxima sexta
        next_friday = today + timedelta(days=offset)

        last_cols = date_cols[-4:] # Pega as últimas 4 colunas de data
        last_dates = []
        for col in last_cols:
            try:
                d = dt.fromisoformat(str(col))
            except ValueError:
                d = pd.to_datetime(col)
            last_dates.append(d.date())

        def prever_alvo(row):
            """
            Prevê o valor alvo usando regressão linear simples.
            """
            ys = [row[c] for c in last_cols]
            if any(pd.isnull(ys)):
                return None
            xs = [d.toordinal() for d in last_dates]
            m, b = np.polyfit(xs, ys, 1)
            return round(m * next_friday.toordinal() + b, 2)

        df['Alvo'] = df.apply(prever_alvo, axis=1)

        # Filtro de ticker para o usuário
        opt = df["Ticker"].unique().tolist()
        sel = st.multiselect("Filtrar por Ticker:", options=opt, default=[])
        if sel: df = df[df["Ticker"].isin(sel)]

        # Oculta colunas especificadas
        ocultar = [col for col in hidden_cols if col in df.columns] if hidden_cols else []
        display_df = df.drop(columns=ocultar, errors="ignore")

        # Reordena as colunas para exibir "Cotação atual" após "Ticker_YF"
        cols = list(display_df.columns)
        if "Ticker_YF" in cols and "Cotação atual" in cols:
            cols.remove("Cotação atual"); i = cols.index("Ticker_YF"); cols.insert(i+1,"Cotação atual")
            display_df = display_df[cols]

        # Formata as colunas numéricas
        fmt = {col: "{:.2f}" for col in display_df.select_dtypes(include=[np.number]).columns}

        display_df.columns = [str(c) for c in display_df.columns]
        date_cols_fmt = [c for c in display_df.columns if c[:4].isdigit() and "-" in c]
        date_cols_fmt = sorted(date_cols_fmt)[-5:] # Pega as últimas 5 colunas de data
        colunas_para_estilo = date_cols_fmt + ["Cotação atual"] if "Cotação atual" in display_df.columns else date_cols_fmt

        def highlight_colunas_comparadas(row):
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
                    if atual > ant:
                        styles[i] = 'color: green; font-weight: bold'
                    elif atual < ant:
                        styles[i] = 'color: red; font-weight: bold'
            return styles

        # Aplica o estilo e exibe o DataFrame
        styled = display_df.style.format(fmt)
        styled = styled.apply(highlight_colunas_comparadas, axis=1, subset=colunas_para_estilo)

        st.subheader("📄 Dados da aba 'Streamlit'")
        st.dataframe(styled)

    except FileNotFoundError:
        st.error(f"❌ O arquivo '{EXCEL_PATH}' não foi encontrado. Certifique-se de que ele está no mesmo diretório da aplicação.")
    except Exception as e:
        st.error(f"❌ Erro ao processar dados: {e}. Por favor, verifique os logs para mais detalhes.")
        # O print abaixo aparecerá nos logs do Render, o que é útil para depuração
        print(f"Erro detalhado: {e}")

if __name__ == "__main__":
    main()
