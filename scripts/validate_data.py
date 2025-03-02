from pyspark.sql import SparkSession
from scripts.data_validation import validate_data

# Criar sessão Spark
spark = SparkSession.builder.appName("Validação de Dados").getOrCreate()

# Caminho dos dados processados
OUTPUT_PATH = "data/dados-processados/"

# Carregar os dados processados do Parquet
df = spark.read.parquet(OUTPUT_PATH)

# Validar os dados
validate_data(df)

# Fechar Spark
spark.stop()
