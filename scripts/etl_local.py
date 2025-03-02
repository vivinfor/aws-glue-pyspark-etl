from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, when

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("ETL Local - Fraude Financeira") \
    .getOrCreate()

# Caminhos locais
INPUT_PATH = "data/dados-brutos.csv"
OUTPUT_PATH = "data/dados-processados/"

# Ler CSV com separador "|"
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True, sep="|")

# Remover duplicatas e valores nulos
df = df.dropDuplicates()
df = df.na.drop()

# Identificar outliers no valor da transação usando Z-score
df = df.withColumn("z_score", (col("amt") - mean("amt").over()) / stddev("amt").over())
df = df.filter(col("z_score").between(-3, 3))  # Mantém valores dentro de 3 desvios padrão
df = df.drop("z_score")  # Remover coluna temporária

# Salvar os dados processados em formato Parquet
df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("ETL Finalizado com Sucesso!")
spark.stop()
