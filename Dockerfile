# Base image
FROM python:3.8-slim

# Definir diretório de trabalho
WORKDIR /app

# Copiar arquivos necessários
COPY requirements.txt ./
COPY scripts/ ./scripts/
COPY data/ ./data/

# Instalar dependências
RUN pip install --no-cache-dir -r requirements.txt

# Rodar o ETL
CMD ["python", "scripts/etl_local.py"]