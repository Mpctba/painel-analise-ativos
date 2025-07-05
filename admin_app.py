# admin_app.py

import os
import streamlit as st
import pandas as pd
import yfinance as yf
import unicodedata
import numpy as np
from datetime import date, timedelta, datetime as dt

# Configurações da página
st.set_page_config(page_title="📈 Análise de Preços Semanais - BOV2025", layout="wide")
st.title("📈 Análise de Preços Semanais - BOV2025")

# Constantes
EXCEL_PATH = "BOV2025_Analise_Completa_B.xlsx"
SHEET_NAME = "Streamlit"
HIDDEN_FILES = ["hidden_cols.txt", "hidden_col.txt"]

# Função de arredondamento seguro
def safe_round(x, ndigits=2):
    try:
        return round(x, ndigits)
    except Exception:
        return None

@st.cache_data(ttl=3600)
def get_price_var_min_max_last(ticker_yf: str):
    """
    Retorna preço ajustado (close), variação diária %, mínima/sexta, máxima/sexta e último fechamento de sexta.
    """
    try:
        hist = yf.download(
            tickers=ticker_yf,
            start="2024-06-01",
            tz="America/Sao_Paulo",
            auto_adjust=True,
            progress=False
        )
        if hist.empty or "Close" not in hist:
            return [None]*5

        closes = hist["Close"].dropna()
        if closes.empty:
            return [None]*5

        close_today = safe_round(closes.iloc[-1])
        close_yesterday = closes.iloc[-2] if len(closes) >= 2 else None
        var_pct = safe_round((close_today - close_yesterday) / close_yesterday * 100) if close_yesterday is not None else None

        # Filtra sextas-feiras
        fridays = closes[closes.index.weekday == 4]
        min_f = safe_round(fridays.min()) if not fridays.empty else None
        max_f = safe_round(fridays.max()) if not fridays.empty else None
        last_f = safe_round(fridays.iloc[-1]) if not fridays.empty else None

        return [close_today, var_pct, min_f, max_f, last_f]
    except Exception:
        return [None]*5

@st.cache_data(ttl=300)
def carregar_planilha(path: str, aba: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=aba)
    # Normaliza e remove colunas vazias/Unnamed
    df.columns = [unicodedata.normalize('NFC', str(c).strip()) for c in df.columns]
    df = df.dropna(axis=1, how="all")
    df = df.loc[:, ~df.columns.str.match(r"Unnamed:")]
    return df


def main():
    try:
        df = carregar_planilha(EXCEL_PATH, SHEET_NAME)

        # Ler colunas ocultas
        hidden_cols = []
        for fname in HIDDEN_FILES:
            if os.path.exists(fname):
                hidden_cols = [l.strip() for l in open(fname, encoding="utf-8") if l.strip()]
                break

        # Valida ticker
        if "Ticker" not in df.columns:
            st.warning("Coluna 'Ticker' não encontrada.")
            return

        # Formata tickers para yfinance (.SA)
        df["Ticker_YF"] = df["Ticker"].astype(str).str.strip() + ".SA"

        # Busca métricas de preço
        metrics = df["Ticker_YF"].apply(lambda t: pd.Series(get_price_var_min_max_last(t)))
        metrics.columns = [
            "Cotação atual", "Var", "Mínima sexta", "Máxima sexta", "Último fechamento sexta"
        ]
        df = pd.concat([df, metrics], axis=1)

        # Filtro por Ticker com multiselect
        options = df["Ticker"].unique().tolist()
        selected = st.multiselect("Filtrar por Ticker:", options=options, default=options)
        if selected:
            df = df[df["Ticker"].isin(selected)]

        # SR: Suporte/Resistência
        def calc_sr(row):
            H, L, C = row["Máxima sexta"], row["Mínima sexta"], row["Último fechamento sexta"]
            if pd.notnull(H) and pd.notnull(L) and pd.notnull(C):
                P = (H + L + C) / 3
                vals = [L - 2*(H-P), P-(H-L), 2*P-H, P, 2*P-L, P+(H-L), H+2*(P-L)]
                return [safe_round(v) for v in vals]
            return [None]*7
        df[["S3","S2","S1","P","R1","R2","R3"]] = df.apply(calc_sr, axis=1, result_type="expand")

        # Níveis próximos
        def proximos(row):
            price = row.get("Cotação atual")
            levels = [row[c] for c in ["S3","S2","S1","P","R1","R2","R3"] if pd.notnull(row[c])]
            if pd.isnull(price) or not levels:
                return [None, None]
            below = max([v for v in levels if v <= price], default=None)
            above = min([v for v in levels if v > price], default=None)
            return [below, above]
        df[["Nível abaixo","Nível acima"]] = df.apply(proximos, axis=1, result_type="expand")

        # Delta e Amplitude
        def compute_delta(row):
            p, a, b = row["Cotação atual"], row["Nível abaixo"], row["Nível acima"]
            if pd.isnull(p) or pd.isnull(a) or pd.isnull(b):
                return None
            d1 = abs((p - a) / p) * 100
            d2 = abs((b - p) / p) * 100
            return safe_round(min(d1, d2))
        df["Delta"] = df.apply(compute_delta, axis=1)

        def compute_amplitude(row):
            a, b = row["Nível abaixo"], row["Nível acima"]
            if pd.notnull(a) and pd.notnull(b) and a != 0:
                return safe_round((b / a - 1) * 100)
            return None
        df["Amplitude"] = df.apply(compute_amplitude, axis=1)

        # Exibição: remove colunas ocultas
        ocultar = [c for c in hidden_cols if c in df.columns]
        display_df = df.drop(columns=ocultar, errors="ignore")

        st.subheader("📄 Tabela de Preços Semanais")
        st.dataframe(display_df)

    except Exception as e:
        st.error(f"❌ Erro ao processar dados: {e}")

if __name__ == "__main__":
    main()
