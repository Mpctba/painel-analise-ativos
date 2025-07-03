# app.py

import streamlit as st
import pandas as pd
import yfinance as yf

st.set_page_config(page_title="📈 Análise de Preços Semanais - BOV2025", layout="wide")
st.title("📈 Análise de Preços Semanais - BOV2025")

EXCEL_PATH = "BOV2025_Analise_Completa_B.xlsx"
SHEET_NAME = "Streamlit"

@st.cache_data(ttl=300)
def carregar_planilha(path: str, aba: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=aba)
    df = df.dropna(axis=1, how="all")
    df.columns = df.columns.map(str)
    df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
    return df

@st.cache_data(ttl=3600)
def get_price_var_min_max_last(ticker_yf: str):
    try:
        ticker_data = yf.Ticker(ticker_yf)
        hist = ticker_data.history(start="2024-06-01")
        if hist.empty:
            return None, None, None, None, None

        recent = hist.tail(2)
        if len(recent) >= 2:
            close_today = recent["Close"].iloc[-1]
            close_yesterday = recent["Close"].iloc[-2]
            var = ((close_today - close_yesterday) / close_yesterday) * 100
        else:
            close_today = recent["Close"].iloc[-1]
            var = None

        sextas = hist[hist.index.weekday == 4]
        min_sexta = sextas["Close"].min() if not sextas.empty else None
        max_sexta = sextas["Close"].max() if not sextas.empty else None
        fechamento_mais_recente = sextas["Close"].iloc[-1] if not sextas.empty else None

        return close_today, var, min_sexta, max_sexta, fechamento_mais_recente
    except Exception:
        return None, None, None, None, None


def main():
    try:
        # Carrega dados
        df = carregar_planilha(EXCEL_PATH, SHEET_NAME)

        # Valida coluna Ticker
        if "Ticker" not in df.columns:
            st.warning("A coluna 'Ticker' não foi encontrada na planilha.")
            st.stop()

        # Adiciona sufixo para Yahoo Finance
        df["Ticker_YF"] = df["Ticker"].astype(str).str.strip() + ".SA"

        # Busca cotações e indicadores
        df[[
            "Cotação atual",
            "Var",
            "Mínima sexta desde jun/24",
            "Máxima sexta desde jun/24",
            "Fechamento mais recente",
        ]] = df["Ticker_YF"].apply(lambda t: pd.Series(get_price_var_min_max_last(t)))

        # Calcula pivots clássicos (S3..R3)
        def calcular_sr(row):
            H, L, C = row["Máxima sexta desde jun/24"], row["Mínima sexta desde jun/24"], row["Fechamento mais recente"]
            if pd.notnull(H) and pd.notnull(L) and pd.notnull(C):
                P = (H + L + C) / 3
                S1 = 2 * P - H
                S2 = P - (H - L)
                S3 = L - 2 * (H - P)
                R1 = 2 * P - L
                R2 = P + (H - L)
                R3 = H + 2 * (P - L)
                return pd.Series([S3, S2, S1, P, R1, R2, R3])
            return pd.Series([None] * 7)

        df[["S3", "S2", "S1", "P", "R1", "R2", "R3"]] = df.apply(calcular_sr, axis=1)

        # Níveis acima/abaixo do preço
        def encontrar_valores_proximos(row):
            preco = row["Cotação atual"]
            if pd.isnull(preco):
                return pd.Series([None, None])

            niveis = [row[n] for n in ["S3", "S2", "S1", "P", "R1", "R2", "R3"] if pd.notnull(row[n])]
            niveis.sort()
            abaixo = None
            acima = None
            for valor in niveis:
                if valor <= preco:
                    abaixo = valor
                elif acima is None:
                    acima = valor
            return pd.Series([abaixo, acima])

        df[["Nível abaixo", "Nível acima"]] = df.apply(encontrar_valores_proximos, axis=1)

        # Distância percentual até o nível
        def calcular_distancia_percentual(row):
            preco, abaixo, acima = row["Cotação atual"], row["Nível abaixo"], row["Nível acima"]
            if pd.isnull(preco):
                return None
            dist_abaixo = abs((preco - abaixo) / preco) * 100 if pd.notnull(abaixo) else None
            dist_acima = abs((acima - preco) / preco) * 100 if pd.notnull(acima) else None
            if dist_abaixo is not None and dist_acima is not None:
                return min(dist_abaixo, dist_acima)
            return dist_abaixo if dist_abaixo is not None else dist_acima

        df["Distância percentual"] = df.apply(calcular_distancia_percentual, axis=1)

        # Amplitude da faixa
        def calcular_amplitude(row):
            abaixo, acima = row["Nível abaixo"], row["Nível acima"]
            if pd.notnull(abaixo) and pd.notnull(acima) and abaixo != 0:
                return (acima / abaixo - 1) * 100
            return None

        df["Amplitude"] = df.apply(calcular_amplitude, axis=1)

        # Faixa K e Spread
        k_divisores = [-2, -3, -5, -9, -17, -33, -65, 65, 33, 17, 9, 5, 3, 2]
        k_colunas = [f"K ({k})" for k in k_divisores]

        def calcular_k_faixa(row):
            amp = row["Amplitude"]
            if pd.notnull(amp):
                return pd.Series([amp / k if k != 0 else None for k in k_divisores])
            return pd.Series([None] * len(k_divisores))

        df[k_colunas] = df.apply(calcular_k_faixa, axis=1)

        def encontrar_var_faixa(row):
            var = row["Var"]
            if pd.isnull(var):
                return pd.Series([None, None])
            k_vals = [row[f"K ({k})"] for k in k_divisores if pd.notnull(row[f"K ({k})"])]
            k_vals.sort()
            abaixo = None
            acima = None
            for valor in k_vals:
                if valor <= var:
                    abaixo = valor
                elif acima is None:
                    acima = valor
            return pd.Series([abaixo, acima])

        df[["Var (abaixo)", "Var (acima)"]] = df.apply(encontrar_var_faixa, axis=1)
        df["Spread (%)"] = df.apply(
            lambda r: r["Var (acima)"] - r["Var (abaixo)"] if pd.notnull(r["Var (acima)"]) and pd.notnull(r["Var (abaixo)"]) else None,
            axis=1,
        )

        # Converte colunas de datas para numérico
        colunas_datas = sorted([col for col in df.columns if col[:4].isdigit() and "-" in col])
        for col in colunas_datas:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Filtro multiselect por Ticker (inicia vazio, carrega tudo sem seleção)
        tickers = df["Ticker"].unique().tolist()
        selecionados = st.multiselect("Filtrar por Ticker:", options=tickers, default=[])
        if selecionados:
            df = df[df["Ticker"].isin(selecionados)]

        # Reorganiza colunas para exibição
        cols = df.columns.tolist()
        if "Ticker_YF" in cols and "Cotação atual" in cols:
            cols.remove("Cotação atual")
            ticker_idx = cols.index("Ticker_YF")
            cols.insert(ticker_idx + 1, "Cotação atual")
        df = df[cols]

        # Exibição
        st.subheader("📄 Dados da aba 'Streamlit' com cotações e análises técnicas")
        st.dataframe(
            df.style.format(
                {
                    "Var": "{:.2f}%",
                    "Distância percentual": "{:.2f}%",
                    "Amplitude": "{:.2f}%",
                    "Var (abaixo)": "{:.2f}%",
                    "Var (acima)": "{:.2f}%",
                    "Spread (%)": "{:.2f}%",
                    **{col: "{:.2f}%" for col in k_colunas}
                }
            )
        )

    except FileNotFoundError:
        st.error(f"❌ O arquivo '{EXCEL_PATH}' não foi encontrado no diretório.")
    except Exception as e:
        st.error(f"❌ Ocorreu um erro ao processar os dados: {e}")

if __name__ == "__main__":
    main()
