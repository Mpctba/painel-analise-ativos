# Usa uma imagem Python leve
FROM python:3.10-slim

# Define o diretório de trabalho
WORKDIR /app

# Copia apenas o requirements.txt para aproveitar o cache
COPY requirements.txt ./

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY . .

# Exponha a porta que o Render usará
EXPOSE 8000

# Comando para rodar o Streamlit (shell form expande $PORT automaticamente)
CMD streamlit run app_admin.py --server.port $PORT --server.address 0.0.0.0
