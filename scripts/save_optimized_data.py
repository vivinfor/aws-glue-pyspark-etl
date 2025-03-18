import os
import subprocess
import yaml
import logging
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, lit
from pyspark.sql.types import DoubleType, IntegerType

# 📌 Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 📂 Carregar Configuração do YAML
config_path = os.path.abspath("config/config.yaml")
if not os.path.exists(config_path):
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

PROCESSED_DATA_DIR = os.path.abspath(config["data_path"])
OPTIMIZED_DATA_DIR = os.path.abspath(config["optimized_data_path"])

logger.info(f"📂 Diretório de dados processados: {PROCESSED_DATA_DIR}")
logger.info(f"📂 Diretório de dados otimizados: {OPTIMIZED_DATA_DIR}")

# ✅ **Executar `data_validation.py` antes de otimizar os dados**
logger.info("🔍 Iniciando validação de dados antes da otimização...")
validation_script = "scripts/data_validation.py"

try:
    result = subprocess.run([sys.executable, validation_script], check=True, text=True, capture_output=True)
    logger.info(f"📊 Resultado da validação:\n{result.stdout}")
except subprocess.CalledProcessError as e:
    logger.error(f"🚨 Erro ao validar os dados! Saída:\n{e.stderr}")
    exit(1)

# 🚀 Criar sessão Spark
spark = SparkSession.builder.appName("Save Optimized Data").getOrCreate()

# 📌 Carregar os dados processados
logger.info("📂 Carregando dados processados após validação...")
df = spark.read.parquet(PROCESSED_DATA_DIR)

# 🔥 Remover colunas desnecessárias
columns_to_remove = {"profile", "acct_num", "time_diff"}
df = df.drop(*columns_to_remove)
logger.info(f"✅ Removidas colunas desnecessárias: {columns_to_remove}")

# 🔥 **Corrigir tipos incorretos**
df = df.withColumn("zip", col("zip").cast(IntegerType()))
df = df.withColumn("lat", col("lat").cast(DoubleType()))
df = df.withColumn("long", col("long").cast(DoubleType()))
df = df.withColumn("city_pop", col("city_pop").cast(IntegerType()))
df = df.withColumn("unix_time", col("unix_time").cast(IntegerType()))
df = df.withColumn("amt", col("amt").cast(DoubleType()))
df = df.withColumn("is_fraud", col("is_fraud").cast(IntegerType()))
df = df.withColumn("merch_lat", col("merch_lat").cast(DoubleType()))
df = df.withColumn("merch_long", col("merch_long").cast(DoubleType()))

logger.info("✅ Correção de tipos aplicada.")

# 📌 **Garantir que valores nulos tenham tratamento adequado**
df = df.fillna({
    "merchant": "Desconhecido",
    "merch_lat": 0.0,
    "merch_long": 0.0
})
logger.info("✅ Valores nulos tratados.")

# 🔍 **Validações antes de salvar**
total_registros = df.count()
total_transacoes_fraude = df.filter(col("is_fraud") == 1).count()
media_amt = df.select(mean("amt")).collect()[0][0]

logger.info(f"📊 Total de registros: {total_registros}")
logger.info(f"📊 Total de transações fraudulentas: {total_transacoes_fraude}")
logger.info(f"📊 Média de valores de transação (amt): {media_amt:.2f}")

# 📌 **Garantir que o diretório otimizado está pronto para salvar os dados**
if os.path.exists(OPTIMIZED_DATA_DIR):
    logger.info(f"🗑️ Diretório existente detectado: {OPTIMIZED_DATA_DIR}. Removendo para recriar...")
    shutil.rmtree(OPTIMIZED_DATA_DIR)

os.makedirs(OPTIMIZED_DATA_DIR)
logger.info(f"📂 Diretório recriado: {OPTIMIZED_DATA_DIR}")

# 📌 **Salvar os dados otimizados em formato Parquet com particionamento por categoria**
df.write.mode("overwrite").partitionBy("category").parquet(OPTIMIZED_DATA_DIR)
logger.info("✅ Dados otimizados salvos com sucesso!")

# 📂 **Testar leitura do dataset salvo**
logger.info("📂 Testando leitura dos dados otimizados...")
df_test = spark.read.parquet(OPTIMIZED_DATA_DIR)
logger.info(f"✅ Registros lidos: {df_test.count()}")
logger.info("🎯 Estrutura do dataset otimizado:")
df_test.printSchema()

logger.info("🚀 Processo de otimização concluído!")
spark.stop()
