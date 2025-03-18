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

# ✅ **Carregar CSV**
df = spark.read.option("sep", "|").option("header", True).csv(INPUT_FILE)

# ✅ **Garantir que os dados pertencem ao ano de 2023**
df = df.filter(col("trans_date_trans_time").between("2023-01-01", "2023-12-31"))

# ✅ **Criar `hour_of_day` corretamente**
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

# 🔹 **Criar `possible_fraud_high_value`**
df = df.withColumn("possible_fraud_high_value", (col("amt") > 10000).cast("integer"))

# 📊 **Criar janela para detecção de transações rápidas**
window_spec_time = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")
df = df.withColumn("time_diff", unix_timestamp("trans_date_trans_time") - lag(unix_timestamp("trans_date_trans_time")).over(window_spec_time))
df = df.fillna({"time_diff": 0})  # Substituir NaN por 0

df = df.withColumn("possible_fraud_fast_transactions", when(col("time_diff") < 10, 1).otherwise(0))

# 🔹 **Garantir que as colunas numéricas tenham o tipo correto**
numeric_columns = ["zip", "lat", "long", "city_pop", "amt", "is_fraud", "merch_lat", "merch_long"]
for col_name in numeric_columns:
    if col_name in df.columns:
        df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

# 📌 **Debug: Mostrar esquema e estatísticas antes de salvar**
logger.info("🔍 Estrutura final do DataFrame:")
df.printSchema()

# 🔍 **Confirmar que todas as colunas do esquema esperado foram criadas**
expected_columns = {field["name"] for field in schema_json["fields"]}
actual_columns = set(df.columns)
missing_columns = expected_columns - actual_columns

if missing_columns:
    logger.warning(f"⚠️ As seguintes colunas não foram geradas: {missing_columns}")

# 🔍 **Confirmar distribuição de `transaction_period`**
logger.info("🔍 Verificando distribuição de `transaction_period`:")
df.select("transaction_period").groupby("transaction_period").count().show()

# 🔍 **Checar se `day_of_week` realmente foi gerado corretamente**
if "day_of_week" in df.columns:
    logger.info("🔍 Exemplo de registros para validação:")
    df.select("day_of_week", "hour_of_day", "transaction_period", "possible_fraud_high_value", "possible_fraud_fast_transactions").show(10)
else:
    logger.warning("⚠️ `day_of_week` não foi criada corretamente e não será exibida.")

# 🔍 **Checagem de valores nulos antes de salvar**
for col_name in expected_columns:
    null_count = df.filter(col(col_name).isNull()).count()
    if null_count > 0:
        logger.warning(f"⚠️ {null_count} registros possuem valores nulos na coluna `{col_name}`.")

# 📂 **Salvar dados processados**
logger.info("📂 Salvando dados processados...")
df.write.mode("overwrite").parquet(OUTPUT_PATH)
logger.info("✅ Dados processados salvos com sucesso!")

# 🚀 **Encerrar sessão**
spark.stop()
logger.info("🚀 ETL Finalizado!")
