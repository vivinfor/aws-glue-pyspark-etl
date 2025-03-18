import os
import yaml
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType, IntegerType

# 📂 Carregar Configuração do YAML
config_path = os.path.abspath("config/config.yaml")
print(f"📂 Tentando carregar: {config_path}")

if os.path.exists(config_path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    print("✅ Configuração carregada com sucesso!")
else:
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")

# 📂 Definir caminhos usando o config.yaml
PROCESSED_DATA_DIR = os.path.normpath(config.get("data_path", "data/processed/"))
OPTIMIZED_DATA_DIR = os.path.normpath(config.get("optimized_data_path", "data/optimized/"))

if not os.path.isabs(PROCESSED_DATA_DIR):
    PROCESSED_DATA_DIR = os.path.abspath(PROCESSED_DATA_DIR)
if not os.path.isabs(OPTIMIZED_DATA_DIR):
    OPTIMIZED_DATA_DIR = os.path.abspath(OPTIMIZED_DATA_DIR)

print(f"📂 Diretório de dados processados: {PROCESSED_DATA_DIR}")
print(f"📂 Diretório de dados otimizados: {OPTIMIZED_DATA_DIR}")

# 🚀 Criar sessão Spark
spark = SparkSession.builder.appName("Save Optimized Data").getOrCreate()

# 📌 Ler os dados processados
print("📂 Carregando dados processados...")
df = spark.read.parquet(PROCESSED_DATA_DIR)
print(f"✅ Total de registros carregados: {df.count()}")

# 📌 Colunas esperadas conforme o schema atualizado
expected_columns = {
    "ssn", "trans_date_trans_time", "cc_num", "merchant", "category", "amt",
    "first", "last", "gender", "street", "city", "state", "zip", "lat", "long",
    "city_pop", "job", "dob", "trans_num", "unix_time", "merch_lat", "merch_long",
    "is_fraud", "day_of_week", "hour_of_day", "transaction_period",
    "possible_fraud_high_value", "possible_fraud_fast_transactions"
}
actual_columns = set(df.columns)

# 🔥 Verificar colunas inesperadas e removê-las
extra_columns = actual_columns - expected_columns
if extra_columns:
    print(f"⚠️ Removendo colunas inesperadas: {extra_columns}")
    df = df.drop(*extra_columns)

# 🔥 Verificar colunas faltantes
missing_columns = expected_columns - actual_columns
if missing_columns:
    raise ValueError(f"❌ Colunas faltantes no dataset processado: {missing_columns}")

# ✅ **Correção: Garantir tipos corretos**
df = df.withColumn("amt", col("amt").cast(DoubleType()))
df = df.withColumn("day_of_week", col("day_of_week").cast(StringType()))
df = df.withColumn("hour_of_day", col("hour_of_day").cast(IntegerType()))
df = df.withColumn("possible_fraud_high_value", col("possible_fraud_high_value").cast(IntegerType()))
df = df.withColumn("possible_fraud_fast_transactions", col("possible_fraud_fast_transactions").cast(IntegerType()))
df = df.withColumn("category", col("category").cast(StringType()))
print("🔄 Conversão aplicada aos campos de tipo!")

# 📌 Garantir que o diretório otimizado está pronto para salvar os dados
if os.path.exists(OPTIMIZED_DATA_DIR):
    print(f"🗑️ Diretório existente detectado: {OPTIMIZED_DATA_DIR}. Removendo para recriar...")
    shutil.rmtree(OPTIMIZED_DATA_DIR)

os.makedirs(OPTIMIZED_DATA_DIR)
print(f"📂 Diretório recriado: {OPTIMIZED_DATA_DIR}")

# 📌 Salvar os dados otimizados em formato Parquet com particionamento por categoria
df.write.mode("overwrite").partitionBy("category").parquet(OPTIMIZED_DATA_DIR)
print("✅ Dados otimizados salvos com sucesso!")

# 📂 Testar leitura do dataset salvo
print("📂 Testando leitura dos dados otimizados...")
df_test = spark.read.parquet(OPTIMIZED_DATA_DIR)
print(f"✅ Registros lidos: {df_test.count()}")
print("🎯 Estrutura do dataset otimizado:")
df_test.printSchema()

print("🚀 Processo de otimização concluído!")
spark.stop()
