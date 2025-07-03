# app.py

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
SHEET_NAME = "Streamlit"
HIDDEN_FILES = ["hidden_cols.txt", "hidden_col.txt"]

@st.cache_data(ttl=300)
def carregar_planilha(path: str, aba: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=aba)
    df.columns = [unicodedata.normalize('NFC', str(col).strip()) for col in df.columns]
    df = df.dropna(axis=1, how="all")
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
            close_today = round(recent["Close"].iloc[-1], 2)
            close_yesterday = recent["Close"].iloc[-2]
            var = round(((close_today - close_yesterday) / close_yesterday) * 100, 2)
        else:
            close_today = round(recent["Close"].iloc[-1], 2)
            var = None

        sextas = hist[hist.index.weekday == 4]
        min_sexta = round(sextas["Close"].min(), 2) if not sextas.empty else None
        max_sexta = round(sextas["Close"].max(), 2) if not sextas.empty else None
        fechamento_mais_recente = round(sextas["Close"].iloc[-1], 2) if not sextas.empty else None

        return close_today, var, min_sexta, max_sexta, fechamento_mais_recente
    except Exception:
        return None, None, None, None, None

def main():
    try:
        df = carregar_planilha(EXCEL_PATH, SHEET_NAME)

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

        df["Ticker_YF"] = df["Ticker"].astype(str).str.strip() + ".SA"

        df[[
            "Cotação atual",
            "Var",
            "Mínima sexta desde jun/24",
            "Máxima sexta desde jun/24",
            "Fechamento mais recente",
        ]] = df["Ticker_YF"].apply(lambda t: pd.Series(get_price_var_min_max_last(t)))

        def calcular_sr(row):
            H = row.get("Máxima sexta desde jun/24")
            L = row.get("Mínima sexta desde jun/24")
            C = row.get("Fechamento mais recente")
            if pd.notnull(H) and pd.notnull(L) and pd.notnull(C):
                P = (H + L + C) / 3
                return pd.Series([round(L - 2*(H-P), 2), round(P-(H-L), 2), round(2*P-H, 2), round(P, 2), round(2*P-L, 2), round(P+(H-L), 2), round(H+2*(P-L), 2)])
            return pd.Series([None]*7)

        df[["S3","S2","S1","P","R1","R2","R3"]] = df.apply(calcular_sr, axis=1)

        def encontrar_valores_proximos(row):
            preco = row.get("Cotação atual")
            niveis = [row.get(k) for k in ["S3","S2","S1","P","R1","R2","R3"] if pd.notnull(row.get(k))]
            niveis.sort()
            abaixo = max([v for v in niveis if v<=preco], default=None)
            acima = min([v for v in niveis if v>preco], default=None)
            return pd.Series([abaixo,acima])

        df[["Nível abaixo","Nível acima"]] = df.apply(encontrar_valores_proximos, axis=1)

        def calcular_distancia_percentual(row):
            preco = row.get("Cotação atual")
            abaixo = row.get("Nível abaixo")
            acima  = row.get("Nível acima")
            d1 = abs((preco-abaixo)/preco)*100 if pd.notnull(abaixo) else None
            d2 = abs((acima-preco)/preco)*100 if pd.notnull(acima) else None
            return round(min([d for d in [d1,d2] if d is not None], default=None), 2) if d1 or d2 else None

        df.rename(columns={"Distância percentual": "Delta"}, inplace=True)
        df["Delta"] = df.apply(calcular_distancia_percentual, axis=1)
        df["Amplitude"] = df.apply(lambda r: round(((r.get("Nível acima")/r.get("Nível abaixo")-1)*100), 2) if pd.notnull(r.get("Nível abaixo")) and r.get("Nível abaixo")!=0 else None, axis=1)

        k_div = [-2,-3,-5,-9,-17,-33,-65,65,33,17,9,5,3,2]
        k_cols = [f"K ({k})" for k in k_div]
        df[k_cols] = df["Amplitude"].apply(lambda amp: pd.Series([round(amp/k, 2) if pd.notnull(amp) else None for k in k_div]))

        def encontrar_var_faixa(row):
            var = row.get("Var")
            arr = sorted([row.get(c) for c in k_cols if pd.notnull(row.get(c))])
            aba = max([v for v in arr if v<=var], default=None)
            ac  = min([v for v in arr if v> var], default=None)
            return pd.Series([aba,ac])

        df[["Var (abaixo)","Var (acima)"]] = df.apply(encontrar_var_faixa, axis=1)
        df["Spread (%)"] = df.apply(lambda r: round(r.get("Var (acima)")-r.get("Var (abaixo)"), 2) if pd.notnull(r.get("Var (abaixo)")) else None, axis=1)

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

        def prever_alvo(row):
            ys = [row[c] for c in last_cols]
            if any(pd.isnull(ys)):
                return None
            xs = [d.toordinal() for d in last_dates]
            m, b = np.polyfit(xs, ys, 1)
            return round(m * next_friday.toordinal() + b, 2)

        df['Alvo'] = df.apply(prever_alvo, axis=1)

        opt = df["Ticker"].unique().tolist()
        sel = st.multiselect("Filtrar por Ticker:", options=opt, default=[])
        if sel: df = df[df["Ticker"].isin(sel)]

        ocultar = [col for col in hidden_cols if col in df.columns] if hidden_cols else []
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

        def highlight_colunas_comparadas(row):
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

        styled = display_df.style.format(fmt)
        styled = styled.apply(highlight_colunas_comparadas, axis=1, subset=colunas_para_estilo)

        st.subheader("📄 Dados da aba 'Streamlit'")
        st.dataframe(styled)

    except FileNotFoundError:
        st.error(f"❌ O arquivo '{EXCEL_PATH}' não foi encontrado.")
    except Exception as e:
        st.error(f"❌ Erro ao processar dados: {e}")

if __name__ == "__main__":
    main()
