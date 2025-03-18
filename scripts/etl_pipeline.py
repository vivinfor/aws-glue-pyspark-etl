import os
import json
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, date_format, unix_timestamp, lag, concat, lit, to_timestamp, regexp_extract
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

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

# ✅ **Converter JSON para Schema PySpark**
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

# ✅ **Carregar CSV, ignorando diferença de colunas**
df = spark.read.option("sep", "|").option("header", True).csv(INPUT_FILE)

# ✅ **Verificar formato de trans_time**
logger.info("🔍 Exemplo de valores únicos de `trans_time` para validação:")
df.select("trans_time").distinct().show(10, False)

# ✅ **Detectar se o formato é 12h (AM/PM)**
is_12h_format = df.filter(col("trans_time").rlike("(?i)(AM|PM)")).count() > 0

if is_12h_format:
    logger.warning("⚠️ `trans_time` está no formato 12h (AM/PM). Convertendo para 24h...")
    df = df.withColumn(
        "trans_time",
        when(col("trans_time").rlike("(?i)PM") & (col("trans_time").substr(1, 2).cast("int") < 12),
             (col("trans_time").substr(1, 2).cast("int") + 12).cast("string"))
        .otherwise(col("trans_time"))
    )

# ✅ **Criar `trans_date_trans_time` corretamente**
if "trans_date" in df.columns and "trans_time" in df.columns:
    df = df.withColumn(
        "trans_date_trans_time",
        to_timestamp(concat(col("trans_date"), lit(" "), col("trans_time")), "yyyy-MM-dd HH:mm:ss")
    ).drop("trans_date", "trans_time")

# ✅ **Converter tipos para corresponder ao schema**
for field in schema_json["fields"]:
    col_name = field["name"]
    expected_type = field["type"]

    if col_name in df.columns:
        if expected_type == "int":
            df = df.withColumn(col_name, col(col_name).cast("int"))
        elif expected_type == "double":
            df = df.withColumn(col_name, col(col_name).cast("double"))
        elif expected_type == "timestamp":
            df = df.withColumn(col_name, col(col_name).cast("timestamp"))

# ✅ **Criar `hour_of_day`**
df = df.withColumn("hour_of_day", date_format(col("trans_date_trans_time"), "HH").cast("int"))

# 🔍 **Verificar distribuição dos valores de `hour_of_day`**
logger.info("🔍 Distribuição dos valores de `hour_of_day` após conversão:")
df.select("hour_of_day").groupby("hour_of_day").count().show(24)

# 🔹 **Criar `transaction_period`**
df = df.withColumn(
    "transaction_period",
    when((col("hour_of_day") >= 0) & (col("hour_of_day") < 6), "Madrugada")
    .when((col("hour_of_day") >= 6) & (col("hour_of_day") < 12), "Manhã")
    .when((col("hour_of_day") >= 12) & (col("hour_of_day") < 18), "Tarde")
    .otherwise("Noite")
)

# 🔹 **Criar `possible_fraud_high_value`**
df = df.withColumn("possible_fraud_high_value", (col("amt") > 10000).cast("integer"))

# 📊 **Criar janela para detecção de transações rápidas**
window_spec_time = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")
df = df.withColumn("time_diff", unix_timestamp("trans_date_trans_time") - lag(unix_timestamp("trans_date_trans_time")).over(window_spec_time))
df = df.fillna({"time_diff": 0})  # Substituir NaN por 0

df = df.withColumn("possible_fraud_fast_transactions", when(col("time_diff") < 10, 1).otherwise(0))

# 📌 **Debug: Mostrar esquema e estatísticas antes de salvar**
logger.info("🔍 Estrutura final do DataFrame:")
df.printSchema()

logger.info("🔍 Verificando distribuição de `transaction_period`:")
df.select("transaction_period").groupby("transaction_period").count().show()

logger.info("🔍 Exemplo de registros para validação:")
df.select("day_of_week", "hour_of_day", "transaction_period", "possible_fraud_high_value", "possible_fraud_fast_transactions").show(10)

# 📂 **Salvar dados processados**
logger.info("📂 Salvando dados processados...")
df.write.mode("overwrite").parquet(OUTPUT_PATH)
logger.info("✅ Dados processados salvos com sucesso!")

# 🚀 **Encerrar sessão**
spark.stop()
logger.info("🚀 ETL Finalizado!")
