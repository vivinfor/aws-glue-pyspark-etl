import os
import json
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, date_format, unix_timestamp, lag, concat, lit, to_timestamp, count, mean, stddev
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# 📌 Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 📂 Carregar Configuração do YAML e Schema
CONFIG_PATH = "config/config.yaml"
SCHEMA_PATH = "config/schema.json"
VALIDATION_RULES_PATH = "config/validation_rules.yaml"

for path in [CONFIG_PATH, SCHEMA_PATH, VALIDATION_RULES_PATH]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Arquivo '{path}' não encontrado!")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

with open(SCHEMA_PATH, "r") as f:
    schema_json = json.load(f)

with open(VALIDATION_RULES_PATH, "r") as f:
    validation_rules = yaml.safe_load(f)

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

# ✅ **Carregar CSV**
df = spark.read.option("sep", "|").option("header", True).csv(INPUT_FILE)

# ✅ **Criar `trans_date_trans_time` corretamente**
if "trans_date" in df.columns and "trans_time" in df.columns:
    df = df.withColumn(
        "trans_date_trans_time",
        to_timestamp(concat(col("trans_date"), lit(" "), col("trans_time")), "yyyy-MM-dd HH:mm:ss")
    ).drop("trans_date", "trans_time")

# ✅ **Criar `hour_of_day`**
df = df.withColumn("hour_of_day", date_format(col("trans_date_trans_time"), "HH").cast("int"))

# 🔹 **Criar `transaction_period`**
df = df.withColumn(
    "transaction_period",
    when((col("hour_of_day") >= 0) & (col("hour_of_day") < 6), "Madrugada")
    .when((col("hour_of_day") >= 6) & (col("hour_of_day") < 12), "Manhã")
    .when((col("hour_of_day") >= 12) & (col("hour_of_day") < 18), "Tarde")
    .otherwise("Noite")
)

# 🔹 **Criar `day_of_week` corretamente**
df = df.withColumn(
    "day_of_week",
    when(col("trans_date_trans_time").isNotNull(), date_format(col("trans_date_trans_time"), "E"))
    .otherwise(lit("Erro - Data Inválida"))
)

# ✅ **Aplicar regras de validação configuráveis**
for col_name in validation_rules["validation"]["missing_values"]["critical"]:
    df = df.dropna(subset=[col_name])

fill_values = validation_rules["validation"]["missing_values"]["non_critical"]
df = df.fillna(fill_values)
logger.info("✅ Valores nulos tratados conforme regras configuráveis.")

# ✅ **Detectar outliers em `amt`**
if validation_rules["validation"]["outlier_detection"]["amt"]["method"] == "zscore":
    threshold = validation_rules["validation"]["outlier_detection"]["amt"]["threshold"]
    amt_stats = df.select(mean("amt").alias("mean_amt"), stddev("amt").alias("std_amt")).collect()[0]
    mean_amt, std_amt = amt_stats["mean_amt"], amt_stats["std_amt"]
    df = df.filter((col("amt") <= mean_amt + threshold * std_amt) & (col("amt") >= mean_amt - threshold * std_amt))

logger.info("✅ Outliers removidos conforme regras configuráveis.")

# 📂 **Salvar dados processados**
logger.info("📂 Salvando dados processados...")
df.write.mode("overwrite").parquet(OUTPUT_PATH)
logger.info("✅ Dados processados salvos com sucesso!")

# 🚀 **Encerrar sessão**
spark.stop()
logger.info("🚀 ETL Finalizado!")
