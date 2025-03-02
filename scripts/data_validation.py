import json
import pandas as pd
from pyspark.sql import SparkSession

# Criar sessão Spark
spark = SparkSession.builder.appName("Validação de Dados - ETL").getOrCreate()

# Caminhos dos arquivos
SCHEMA_PATH = "data/schema.json"
DATA_PATH = "data/dados-brutos.csv"

# Carregar o schema
with open(SCHEMA_PATH, "r") as f:
    schema = json.load(f)

# Ler os dados
try:
    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True, sep="|")
except Exception as e:
    print(f"Erro ao carregar os dados: {e}")
    exit()

# Criar dicionário de schema esperado
expected_schema = {field["name"]: field["type"] for field in schema["fields"]}

# Validar se todas as colunas esperadas existem nos dados
missing_columns = [col for col in expected_schema.keys() if col not in df.columns]
if missing_columns:
    print(f"⚠️ Colunas ausentes no dataset: {missing_columns}")
else:
    print("✅ Todas as colunas esperadas estão presentes.")

# Validar tipos de dados
for field in schema["fields"]:
    col_name = field["name"]
    expected_type = field["type"]
    
    if col_name in df.columns:
        actual_type = df.select(col_name).schema[0].dataType.simpleString()
        if expected_type == "double":
            expected_type = "float"
        if actual_type != expected_type:
            print(f"⚠️ Tipo incorreto na coluna '{col_name}': Esperado {expected_type}, encontrado {actual_type}")

# Contar valores nulos
null_counts = df.select([pd.functions.count(pd.functions.when(pd.functions.col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# Contar registros duplicados
duplicate_count = df.count() - df.dropDuplicates().count()
if duplicate_count > 0:
    print(f"⚠️ Existem {duplicate_count} registros duplicados no dataset.")
else:
    print("✅ Nenhuma duplicata encontrada.")

print("📊 Validação de dados concluída!")
spark.stop()
