import os
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, date_format, unix_timestamp, lag, concat, lit, mean, stddev
from pyspark.sql.window import Window

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import os
import yaml

# 📂 Carregar Configuração do YAML
config_path = os.path.abspath("config/config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# Definir ambiente (Local ou AWS)
IS_AWS = config.get("environment") == "aws"

# 📂 Definir caminhos de entrada e saída
if IS_AWS:
    INPUT_PATH = config.get("aws_s3_input")
    OUTPUT_PATH = config.get("aws_s3_output")
else:
    INPUT_PATH = os.path.abspath(config.get("raw_data_path"))
    OUTPUT_PATH = os.path.abspath(config.get("data_path"))

print(f"📂 Caminho de entrada: {INPUT_PATH}")
print(f"📂 Caminho de saída: {OUTPUT_PATH}")


# Criar sessão Spark
if IS_AWS:
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.context import SparkContext
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger.info("🚀 Executando no AWS Glue.")
else:
    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
    logger.info("💻 Executando localmente no PySpark.")

# 📂 Carregar os dados
logger.info("📂 Carregando dados...")
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True, sep="|")
logger.info(f"✅ Total de registros carregados: {df.count()}")

# 🔄 Remover duplicatas
df = df.dropDuplicates()

# 🚀 Remover registros com colunas críticas nulas
df = df.na.drop(subset=["cc_num", "amt", "is_fraud"])
logger.info(f"📊 Registros após remoção de valores nulos críticos: {df.count()}")

# 🔹 Preencher valores nulos opcionais
df = df.fillna({
    "merchant": "Desconhecido",
    "city": "Não informado",
    "state": "Não informado",
    "lat": 0.0,
    "long": 0.0
})

# 🧹 **Filtrar Outliers com Z-score** (caso habilitado)
if config.get("use_z_score_filter", False):
    logger.info("🚀 Aplicando filtro de outliers (Z-score)...")
    window_spec = Window.partitionBy("category").orderBy("amt")
    df = df.withColumn("z_score", (col("amt") - mean(col("amt")).over(window_spec)) / stddev(col("amt")).over(window_spec))
    df = df.filter(col("z_score").between(-3, 3)).drop("z_score")
    logger.info(f"📊 Registros após remoção de outliers: {df.count()}")

# 🔹 Criar colunas adicionais
df = df.withColumn("trans_date_trans_time", concat(col("trans_date"), lit(" "), col("trans_time")).cast("timestamp"))
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

# 📊 Criar janela para detecção de transações rápidas
window_spec_time = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")
df = df.withColumn("time_diff", unix_timestamp("trans_date_trans_time") - lag(unix_timestamp("trans_date_trans_time")).over(window_spec_time))
df = df.withColumn("possible_fraud_fast_transactions", (col("time_diff") < 10).cast("integer"))

# 🚀 Configurar compressão e particionamento
compression_codec = config.get("compression", "snappy")
spark.conf.set("spark.sql.parquet.compression.codec", compression_codec)
partition_keys = config.get("partition_keys", ["category"])

# 📂 Criar diretório de saída se for local
if not IS_AWS and not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

# 💾 Salvar dados processados
logger.info("📂 Salvando dados processados...")
df.write.mode("overwrite").partitionBy(*partition_keys).parquet(OUTPUT_PATH)
logger.info("✅ Dados processados salvos com sucesso!")

# 🚀 Encerrar sessão
spark.stop()
logger.info("🚀 ETL Finalizado!")
