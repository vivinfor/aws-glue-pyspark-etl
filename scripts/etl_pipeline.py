import os
import json
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, date_format, unix_timestamp, lag, concat, lit, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# 📌 Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 📂 Carregar Configuração do YAML e Schema
config_path = os.path.abspath("config/config.yaml")
schema_path = os.path.abspath("config/schema.json")

if not os.path.exists(config_path):
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")

if not os.path.exists(schema_path):
    raise FileNotFoundError("❌ Arquivo 'schema.json' não encontrado!")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

with open(schema_path, "r") as f:
    schema_json = json.load(f)

# 📌 Converter schema JSON para PySpark StructType
def json_to_spark_schema(json_schema):
    fields = []
    for field in json_schema["fields"]:
        field_type = field["type"]
        nullable = field.get("nullable", True)

        if field_type == "string":
            spark_type = StringType()
        elif field_type == "double":
            spark_type = DoubleType()
        elif field_type == "int":
            spark_type = IntegerType()
        elif field_type == "timestamp":
            spark_type = TimestampType()
        else:
            raise ValueError(f"⚠️ Tipo de dado não suportado: {field_type}")

        fields.append(StructField(field["name"], spark_type, nullable))

    return StructType(fields)

schema = json_to_spark_schema(schema_json)

# 📂 Definir ambiente (Local ou AWS)
IS_AWS = config.get("environment") == "aws"

# 📂 Definir caminhos de entrada e saída
INPUT_PATH = os.path.abspath(config["aws_s3_input"] if IS_AWS else config["raw_data_path"])
OUTPUT_PATH = os.path.abspath(config["aws_s3_output"] if IS_AWS else config["data_path"])

logger.info(f"📂 Caminho de entrada: {INPUT_PATH}")
logger.info(f"📂 Caminho de saída: {OUTPUT_PATH}")

# 📌 Criar sessão Spark
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .getOrCreate()
logger.info("💻 Executando localmente no PySpark.")

# 📂 Selecionar o primeiro arquivo CSV
csv_files = [f for f in os.listdir(INPUT_PATH) if f.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError(f"❌ Nenhum arquivo CSV válido encontrado em '{INPUT_PATH}'.")

INPUT_FILE = os.path.join(INPUT_PATH, csv_files[0])
logger.info(f"📂 Arquivo selecionado: {INPUT_FILE}")

# 📌 Carregar CSV com schema correto e verificar se `trans_time` foi carregada corretamente
df = spark.read.option("sep", "|").csv(INPUT_FILE, header=True, schema=schema)

# 🔹 Exibir nomes das colunas carregadas
logger.info(f"📊 Colunas carregadas: {df.columns}")

# 🔹 Se `trans_date_trans_time` já existe, não tentar recriá-la
if "trans_date_trans_time" not in df.columns:
    # Verificar se `trans_date` e `trans_time` existem
    if "trans_date" in df.columns and "trans_time" in df.columns:
        df = df.withColumn(
            "trans_date_trans_time",
            to_timestamp(concat(col("trans_date"), lit(" "), col("trans_time")), "yyyy-MM-dd HH:mm:ss")
        )
    else:
        raise ValueError("❌ As colunas 'trans_date' e 'trans_time' não foram carregadas corretamente!")

# 🔹 Criar `day_of_week`
df = df.withColumn(
    "day_of_week",
    when(col("trans_date_trans_time").isNotNull(), date_format(col("trans_date_trans_time"), "E"))
    .otherwise(lit("Erro - Data Inválida"))
)

# 🔹 Criar `hour_of_day`
df = df.withColumn("hour_of_day", date_format(col("trans_date_trans_time"), "HH").cast("int"))

# 🔹 Criar `transaction_period`
df = df.withColumn(
    "transaction_period",
    when(col("hour_of_day") < 6, "Madrugada")
    .when(col("hour_of_day") < 12, "Manhã")
    .when(col("hour_of_day") < 18, "Tarde")
    .otherwise("Noite")
)

# 🔹 Criar `possible_fraud_high_value`
df = df.withColumn("possible_fraud_high_value", (col("amt") > 10000).cast("integer"))

# 📊 Criar janela para detecção de transações rápidas
window_spec_time = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")
df = df.withColumn("time_diff", unix_timestamp("trans_date_trans_time") - lag(unix_timestamp("trans_date_trans_time")).over(window_spec_time))
df = df.fillna({"time_diff": 0})  # Substitui NaN por 0

df = df.withColumn("possible_fraud_fast_transactions", when(col("time_diff") < 10, 1).otherwise(0))

# 🔹 Configurar compressão e particionamento
compression_codec = config.get("compression", "snappy")
spark.conf.set("spark.sql.parquet.compression.codec", compression_codec)
partition_keys = ["day_of_week", "transaction_period"]

# 📂 Criar diretório de saída se for local
if not IS_AWS and not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)



# 🔹 Verificar se 'trans_date' e 'trans_time' existem antes de processar
if "trans_date" in df.columns and "trans_time" in df.columns:
    df = df.withColumn(
        "trans_date_trans_time",
        to_timestamp(concat(col("trans_date"), lit(" "), col("trans_time")), "yyyy-MM-dd HH:mm:ss")
    )

    df = df.drop("trans_date", "trans_time")


# 💾 Salvar dados processados com particionamento correto
logger.info("📂 Salvando dados processados...")
df.write.mode("overwrite").partitionBy(*partition_keys).parquet(OUTPUT_PATH)
logger.info("✅ Dados processados salvos com sucesso!")

# 🚀 Encerrar sessão
spark.stop()
logger.info("🚀 ETL Finalizado!")
