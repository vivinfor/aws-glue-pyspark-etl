import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_validation import validate_data

from pyspark.sql import SparkSession

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
