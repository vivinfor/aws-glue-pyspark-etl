import os
import sys
import logging
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import (
    col, mean, stddev, when, date_format, unix_timestamp, lag,
    concat, lit, count
)
from pyspark.sql.window import Window

# Configuração do logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Iniciando job do AWS Glue...")

# Criando contexto do AWS Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("aws_glue_etl_job", args=[])

# Configuração de variáveis de ambiente para os caminhos S3
INPUT_PATH = os.getenv("INPUT_PATH", "s3://seu-bucket/dados-brutos.csv")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "s3://seu-bucket/dados-processados/")

logger.info(f"Input Path: {INPUT_PATH}")
logger.info(f"Output Path: {OUTPUT_PATH}")

# Criar cliente Boto3 para gerenciar o S3 (se necessário)
s3_client = boto3.client("s3")

# Ler os dados do S3 como DynamicFrame (AWS Glue)
datasource = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [INPUT_PATH], "recurse": True}
)

# Converter para DataFrame do Spark
df = datasource.toDF()

# Remover duplicatas
df = df.dropDuplicates()

# Remover registros onde colunas críticas sejam nulas
df = df.na.drop(subset=["cc_num", "amt", "is_fraud"])

# Preencher valores nulos em colunas opcionais
df = df.fillna({
    "merchant": "Desconhecido",
    "city": "Não informado",
    "state": "Não informado",
    "lat": 0.0,
    "long": 0.0
})

# Definir uma janela para cálculo estatístico (particionada por categoria)
window_spec = Window.partitionBy("category").orderBy("amt")

# Calcular Z-score corretamente
df = df.withColumn("z_score", (col("amt") - mean(col("amt")).over(window_spec)) / stddev(col("amt")).over(window_spec))

# Filtrar outliers mantendo apenas valores dentro de 3 desvios padrão
df = df.filter(col("z_score").between(-3, 3)).drop("z_score")

# Criar coluna combinando data e hora
df = df.withColumn("trans_date_trans_time", concat(col("trans_date"), lit(" "), col("trans_time")))

# Converter para timestamp
df = df.withColumn("trans_date_trans_time", col("trans_date_trans_time").cast("timestamp"))

# Criar colunas de dia da semana e horário
df = df.withColumn("day_of_week", date_format(col("trans_date_trans_time"), "E"))
df = df.withColumn("hour_of_day", date_format(col("trans_date_trans_time"), "HH").cast("int"))

# Criar coluna categorizando o período da transação
df = df.withColumn(
    "transaction_period",
    when(col("hour_of_day") < 6, "Madrugada")
    .when(col("hour_of_day") < 12, "Manhã")
    .when(col("hour_of_day") < 18, "Tarde")
    .otherwise("Noite")
)

# Criar uma flag para transações acima de 10.000
df = df.withColumn("possible_fraud_high_value", (col("amt") > 10000).cast("integer"))

# Criar uma janela para verificar transações consecutivas do mesmo cartão no mesmo comerciante
window_spec = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")

# Calcular a diferença de tempo entre transações consecutivas
df = df.withColumn("time_diff", unix_timestamp(col("trans_date_trans_time")) - lag(unix_timestamp(col("trans_date_trans_time"))).over(window_spec))

# Criar uma flag para múltiplas transações em menos de 10 segundos
df = df.withColumn("possible_fraud_fast_transactions", (col("time_diff") < 10).cast("integer"))

# Ajuste de tipos de dados para garantir conformidade com o schema esperado
df = df.withColumn("cc_num", col("cc_num").cast("string"))
df = df.withColumn("amt", col("amt").cast("float"))
df = df.withColumn("zip", col("zip").cast("integer"))
df = df.withColumn("lat", col("lat").cast("float"))
df = df.withColumn("long", col("long").cast("float"))
df = df.withColumn("city_pop", col("city_pop").cast("integer"))
df = df.withColumn("dob", col("dob").cast("string"))
df = df.withColumn("unix_time", col("unix_time").cast("integer"))
df = df.withColumn("merch_lat", col("merch_lat").cast("float"))
df = df.withColumn("merch_long", col("merch_long").cast("float"))
df = df.withColumn("is_fraud", col("is_fraud").cast("integer"))
df = df.withColumn("possible_fraud_high_value", col("possible_fraud_high_value").cast("integer"))
df = df.withColumn("possible_fraud_fast_transactions", col("possible_fraud_fast_transactions").cast("integer"))

# Contagem de valores nulos para validação final
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# Configurar compressão Snappy para otimizar armazenamento no S3
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# Contagem final de registros após processamento
logger.info(f"Total de registros processados: {df.count()}")

# Converter para DynamicFrame para AWS Glue
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Salvar os dados processados no S3 com particionamento otimizado
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": OUTPUT_PATH,
        "partitionKeys": ["day_of_week", "transaction_period"]  # Particionamento para otimizar leitura no futuro
    },
    format="parquet"
)

# Finalizar Job
job.commit()

logger.info("ETL no AWS Glue finalizado com sucesso!")
