import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, when, date_format, unix_timestamp, lag, concat, lit, count
from pyspark.sql.window import Window

# 📌 **Carregar Configuração do YAML**
config_path = os.path.abspath("config/config.yaml")
print(f"📂 Tentando carregar: {config_path}")

if os.path.exists(config_path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    print("✅ Configuração carregada com sucesso!")
else:
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")

# 📌 **Definir caminhos usando o config.yaml**
RAW_DATA_DIR = os.path.normpath(config.get("raw_data_path", "data/raw/"))
PROCESSED_DATA_DIR = os.path.normpath(config.get("data_path", "data/processed/"))

# 📌 **Garantir que os caminhos sejam absolutos**
if not os.path.isabs(RAW_DATA_DIR):
    RAW_DATA_DIR = os.path.abspath(RAW_DATA_DIR)
if not os.path.isabs(PROCESSED_DATA_DIR):
    PROCESSED_DATA_DIR = os.path.abspath(PROCESSED_DATA_DIR)

# 📌 **Listar arquivos na pasta raw e selecionar um arquivo CSV**
csv_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError(f"❌ Nenhum arquivo CSV encontrado em '{RAW_DATA_DIR}'.")

# 📌 **Selecionar o primeiro arquivo disponível**
INPUT_PATH = os.path.join(RAW_DATA_DIR, csv_files[0])

print(f"📂 Arquivo de entrada: {INPUT_PATH}")

# 📌 **Verificar se o arquivo existe**
if not os.path.exists(INPUT_PATH):
    raise FileNotFoundError(f"❌ O arquivo '{INPUT_PATH}' não foi encontrado!")

# 📌 **Criar sessão Spark**
spark = SparkSession.builder \
    .appName("ETL Local - Fraude Financeira") \
    .getOrCreate()

# 📌 **Ler CSV com separador '|'**
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True, sep="|")

# 📌 **Remover duplicatas**
df = df.dropDuplicates()

# 📌 **Remover registros onde colunas críticas sejam nulas**
df = df.na.drop(subset=["cc_num", "amt", "is_fraud"])

# 📌 **Preencher valores nulos em colunas opcionais**
df = df.fillna({
    "merchant": "Desconhecido",
    "city": "Não informado",
    "state": "Não informado",
    "lat": 0.0,
    "long": 0.0
})

# 📌 **Definir partições para execução local**
IS_LOCAL = os.getenv("IS_LOCAL", "true").lower() == "true"
if IS_LOCAL:
    df = df.repartition(4)

# 📌 **Criar colunas adicionais**
df = df.withColumn("trans_date_trans_time", concat(col("trans_date"), lit(" "), col("trans_time")))
df = df.withColumn("trans_date_trans_time", col("trans_date_trans_time").cast("timestamp"))
df = df.withColumn("day_of_week", date_format(col("trans_date_trans_time"), "E"))
df = df.withColumn("hour_of_day", date_format(col("trans_date_trans_time"), "HH").cast("int"))

df = df.withColumn(
    "transaction_period",
    when(col("hour_of_day") < 6, "Madrugada")
    .when(col("hour_of_day") < 12, "Manhã")
    .when(col("hour_of_day") < 18, "Tarde")
    .otherwise("Noite")
)

df = df.withColumn("possible_fraud_high_value", (col("amt") > 10000).cast("integer"))

# 📌 **Criar janela de detecção de transações rápidas**
window_spec_time = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")
df = df.withColumn("time_diff", unix_timestamp("trans_date_trans_time") - lag(unix_timestamp("trans_date_trans_time")).over(window_spec_time))
df = df.withColumn("possible_fraud_fast_transactions", (col("time_diff") < 10).cast("integer"))

# 📌 **Criar diretório de saída se não existir**
if not os.path.exists(PROCESSED_DATA_DIR):
    os.makedirs(PROCESSED_DATA_DIR)

# 📌 **Salvar os dados processados em Parquet**
df.write.mode("overwrite").partitionBy("category").parquet(PROCESSED_DATA_DIR)

print("✅ ETL Finalizado com Sucesso!")
spark.stop()
