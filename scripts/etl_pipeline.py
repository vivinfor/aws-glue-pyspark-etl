import os
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, date_format, unix_timestamp, lag, concat, lit, mean, stddev, to_date, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# 📌 Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 📂 Carregar Configuração do YAML
config_path = os.path.abspath("config/config.yaml")
if not os.path.exists(config_path):
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# 📂 Definir ambiente (Local ou AWS)
IS_AWS = config.get("environment") == "aws"

# 📂 Definir caminhos de entrada e saída
if IS_AWS:
    INPUT_PATH = config.get("aws_s3_input")
    OUTPUT_PATH = config.get("aws_s3_output")
else:
    INPUT_PATH = os.path.abspath(config.get("raw_data_path"))
    OUTPUT_PATH = os.path.abspath(config.get("data_path"))

logger.info(f"📂 Caminho de entrada: {INPUT_PATH}")
logger.info(f"📂 Caminho de saída: {OUTPUT_PATH}")

# 📌 Criar sessão Spark
if IS_AWS:
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.context import SparkContext
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    logger.info("🚀 Executando no AWS Glue.")
else:
    spark = SparkSession.builder \
        .appName("ETL Pipeline") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .getOrCreate()
    logger.info("💻 Executando localmente no PySpark.")

# 📂 Listar arquivos CSV válidos
csv_files = [f for f in os.listdir(INPUT_PATH) if f.endswith(".csv") and "exemplo_submissao" not in f]
if not csv_files:
    raise FileNotFoundError(f"❌ Nenhum arquivo CSV válido encontrado em '{INPUT_PATH}'.")

# 📌 Selecionar o primeiro arquivo
INPUT_FILE = os.path.join(INPUT_PATH, csv_files[0])
logger.info(f"📂 Arquivo selecionado: {INPUT_FILE}")

# 📌 Definir esquema correto para leitura do CSV
schema = StructType([
    StructField("ssn", StringType(), True),
    StructField("cc_num", StringType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("city_pop", IntegerType(), True),
    StructField("job", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("acct_num", StringType(), True),
    StructField("profile", StringType(), True),
    StructField("trans_num", StringType(), True),
    StructField("trans_date", StringType(), True),  # 🚀 Garantindo que seja STRING
    StructField("trans_time", StringType(), True),  # 🚀 Garantindo que seja STRING
    StructField("unix_time", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amt", DoubleType(), True),
    StructField("is_fraud", IntegerType(), True),
    StructField("merchant", StringType(), True),
    StructField("merch_lat", DoubleType(), True),
    StructField("merch_long", DoubleType(), True),
])

# 📌 Carregar os dados com o esquema correto
df = spark.read.csv(INPUT_FILE, header=True, schema=schema, sep="|")
df.select("trans_date", "trans_time").show(10, truncate=False)
df.printSchema()

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

# 🔹 Aplicar filtro de outliers com Z-score (caso ativado)
if config.get("use_z_score_filter", False):
    logger.info("🚀 Aplicando filtro de outliers (Z-score)...")
    window_spec = Window.partitionBy("category").orderBy("amt")
    df = df.withColumn("z_score", (col("amt") - mean(col("amt")).over(window_spec)) / stddev(col("amt")).over(window_spec))
    df = df.filter(col("z_score").between(-3, 3)).drop("z_score")
    logger.info(f"📊 Registros após remoção de outliers: {df.count()}")

# 🔹 Criar colunas adicionais corretamente
df = df.withColumn("trans_date_trans_time", to_timestamp(concat(col("trans_date"), lit(" "), col("trans_time")), "yyyy-MM-dd HH:mm:ss"))

df = df.withColumn(
    "day_of_week",
    when(col("trans_date_trans_time").isNotNull(), date_format(col("trans_date_trans_time"), "E"))
    .otherwise(lit("Erro - Data Inválida"))
)

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
df = df.withColumn("time_diff", 
    unix_timestamp("trans_date_trans_time") - lag(unix_timestamp("trans_date_trans_time")).over(window_spec_time)
)
df = df.fillna({"time_diff": 0})  # Substitui NaN por 0

df = df.withColumn("possible_fraud_fast_transactions", 
    when(col("time_diff") < 10, 1).otherwise(0)
)

# 🔹 Configurar compressão e particionamento
compression_codec = config.get("compression", "snappy")
spark.conf.set("spark.sql.parquet.compression.codec", compression_codec)
partition_keys = config.get("partition_keys", ["day_of_week", "transaction_period"])

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
