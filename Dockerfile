# Usa uma imagem Python leve
FROM python:3.10-slim

# Define o diretório de trabalho
WORKDIR /app

# Copia apenas o requirements.txt para aproveitar o cache
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY . .

# Exponha a porta que o Fly.io usará
EXPOSE 8080

# Comando para rodar o Streamlit com o nome correto do script
CMD ["streamlit", "run", "admin_app.py", "--server.port=${PORT}", "--server.address=0.0.0.0"]

