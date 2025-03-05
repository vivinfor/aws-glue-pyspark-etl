import os
import yaml
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType

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

# ✅ **Correção: Garantir que 'amt' e 'day_of_week' estão no tipo correto**
df = df.withColumn("amt", col("amt").cast(DoubleType()))
df = df.withColumn("day_of_week", col("day_of_week").cast(StringType()))  # 🔥 Correção principal
print("🔄 Conversão aplicada aos campos 'amt' e 'day_of_week'")

# 🗑️ Criar diretório otimizado (remover se existir)
if os.path.exists(OPTIMIZED_DATA_DIR):
    shutil.rmtree(OPTIMIZED_DATA_DIR)
    print(f"🗑️ Diretório removido: {OPTIMIZED_DATA_DIR}")

os.makedirs(OPTIMIZED_DATA_DIR)
print(f"📂 Diretório recriado: {OPTIMIZED_DATA_DIR}")

# 📌 Salvar os dados otimizados em formato Parquet com particionamento
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
