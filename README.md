# 📈 Análise de Preços Semanais - BOV2025

Este projeto é um painel interativo desenvolvido com [Streamlit](https://streamlit.io/) para análise semanal de preços de ativos da B3. Ele utiliza dados do Yahoo Finance para calcular níveis de suporte e resistência, variações percentuais, projeções de preço e muito mais.

---

## 🚀 Funcionalidades

- 📊 Importação automática de dados de cotação via `yfinance`
- 🧮 Cálculo de níveis S3 a R3 com base em sextas-feiras desde junho/2024
- 🔍 Identificação de níveis mais próximos da cotação atual
- 📐 Cálculo de amplitude e variações relativas
- 📈 Projeção de preço para a próxima sexta-feira com regressão linear
- 🎯 Destaque visual de evolução semanal
- 📥 Filtro por ticker e ocultação de colunas personalizadas

---

## 🛠️ Tecnologias utilizadas

- Python 3.10+
- Streamlit
- Pandas
- NumPy
- yFinance
- OpenPyXL
- PyTZ

---

## 📦 Instalação local

1. Clone o repositório:

```bash
git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio
