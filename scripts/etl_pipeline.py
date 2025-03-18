import os
import json
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, date_format, unix_timestamp, lag, concat, lit, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

# 📌 Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 📂 Carregar Configuração do YAML e Schema
config_path = os.path.abspath("config/config.yaml")
schema_path = os.path.abspath("config/schema.json")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

with open(schema_path, "r") as f:
    schema_json = json.load(f)

# 📂 Criar Schema PySpark baseado no CSV real
schema = StructType([
    StructField("ssn", StringType(), True),
    StructField("cc_num", StringType(), False),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", IntegerType(), True),
    StructField("lat", FloatType(), True),
    StructField("long", FloatType(), True),
    StructField("city_pop", IntegerType(), True),
    StructField("job", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("acct_num", StringType(), True),
    StructField("profile", StringType(), True),
    StructField("trans_num", StringType(), False),
    StructField("trans_date", StringType(), False),
    StructField("trans_time", StringType(), False),
    StructField("unix_time", IntegerType(), False),
    StructField("category", StringType(), True),
    StructField("amt", FloatType(), False),
    StructField("is_fraud", IntegerType(), False),
    StructField("merchant", StringType(), True),
    StructField("merch_lat", FloatType(), True),
    StructField("merch_long", FloatType(), True)
])

# 📂 Criar sessão Spark
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
logger.info("💻 Executando localmente no PySpark.")

# 📂 Carregar dados
INPUT_PATH = os.path.abspath(config["raw_data_path"])
OUTPUT_PATH = os.path.abspath(config["data_path"])

csv_files = [f for f in os.listdir(INPUT_PATH) if f.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError(f"❌ Nenhum arquivo CSV válido encontrado em '{INPUT_PATH}'.")

INPUT_FILE = os.path.join(INPUT_PATH, csv_files[0])
logger.info(f"📂 Arquivo selecionado: {INPUT_FILE}")

df = spark.read.option("sep", "|").csv(INPUT_FILE, header=True, schema=schema)

# ✅ Garantir que os dados pertencem ao ano de 2023
df = df.filter(col("trans_date").between("2023-01-01", "2023-12-31"))

# ✅ Criar 'trans_date_trans_time'
df = df.withColumn(
    "trans_date_trans_time",
    to_timestamp(concat(col("trans_date"), lit(" "), col("trans_time")), "yyyy-MM-dd HH:mm:ss")
).drop("trans_date", "trans_time")

# ✅ Preencher valores nulos
df = df.fillna({"zip": 0, "merch_lat": 0.0, "merch_long": 0.0, "merchant": "Desconhecido"})

# ✅ Criar 'transaction_period'
df = df.withColumn(
    "transaction_period",
    when(col("hour_of_day") < 6, "Madrugada")
    .when(col("hour_of_day") < 12, "Manhã")
    .when(col("hour_of_day") < 18, "Tarde")
    .otherwise("Noite")
)

# ✅ Validar `is_fraud`
df = df.filter(col("is_fraud").isin([0, 1]))

# 📂 Salvar dados processados
logger.info("📂 Salvando dados processados...")
df.write.mode("overwrite").parquet(OUTPUT_PATH)
logger.info("✅ Dados processados salvos com sucesso!")

# 🚀 Encerrar sessão
spark.stop()
logger.info("🚀 ETL Finalizado!")
