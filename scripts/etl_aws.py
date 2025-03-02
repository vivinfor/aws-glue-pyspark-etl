import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col, mean, stddev

# Criando contexto do AWS Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("aws_glue_etl_job", args=[])

# Caminhos S3
INPUT_PATH = "s3://seu-bucket/dados-brutos.csv"
OUTPUT_PATH = "s3://seu-bucket/dados-processados/"

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

# Tratar valores nulos
df = df.na.drop()

# Identificar e remover outliers com Z-score
df = df.withColumn("z_score", (col("valor") - mean("valor").over()) / stddev("valor").over())
df = df.filter(col("z_score").between(-3, 3))  # Mantém apenas valores aceitáveis

# Converter para DynamicFrame para AWS Glue
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Salvar os dados processados no S3
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": OUTPUT_PATH},
    format="parquet"
)

# Finalizar Job
job.commit()

print("ETL no AWS Glue finalizado com sucesso!")
