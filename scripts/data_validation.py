import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, mean, stddev, unix_timestamp, lag
from pyspark.sql.window import Window

# Criar sessão Spark
spark = SparkSession.builder.appName("Validação de Dados - ETL").getOrCreate()

# Definir caminhos dos arquivos
SCHEMA_PATH = "data/schema.json"
DATA_PATH = "data/dados-processados/"

# Carregar o schema esperado
with open(SCHEMA_PATH, "r") as f:
    schema = json.load(f)

# Criar dicionário de schema esperado
expected_schema = {field["name"]: field["type"] for field in schema["fields"]}

# Ler os dados processados
df = spark.read.parquet(DATA_PATH)

# 1️⃣ **Validar se todas as colunas esperadas existem**
missing_columns = [col for col in expected_schema.keys() if col not in df.columns]
if missing_columns:
    print(f"⚠️ Colunas ausentes no dataset: {missing_columns}")
else:
    print("✅ Todas as colunas esperadas estão presentes.")

# 2️⃣ **Validar tipos de dados conforme `schema.json`**
for field in schema["fields"]:
    col_name = field["name"]
    expected_type = field["type"]
    
    if col_name in df.columns:
        actual_type = df.select(col_name).schema[0].dataType.simpleString()

        # Ajustar caso haja diferença entre float/double
        if expected_type == "double":
            expected_type = "float"

        if actual_type != expected_type:
            print(f"⚠️ Tipo incorreto na coluna '{col_name}': Esperado {expected_type}, encontrado {actual_type}")

# 3️⃣ **Validar valores nulos seguindo as regras de negócio**
null_rules = {
    "cc_num": "Obrigatório",
    "amt": "Obrigatório",
    "merchant": "Preencher com 'Desconhecido'",
    "city": "Preencher com 'Não informado'",
    "state": "Preencher com 'Não informado'",
    "lat": "Preencher com 0.0",
    "long": "Preencher com 0.0",
    "is_fraud": "Obrigatório"
}

for col_name, rule in null_rules.items():
    if col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            if rule == "Obrigatório":
                print(f"🚨 ERRO: {null_count} registros possuem '{col_name}' nulo, registros devem ser descartados!")
            else:
                print(f"🔹 ALERTA: {null_count} registros possuem '{col_name}' nulo. Aplicando regra: {rule}")

# 4️⃣ **Detectar outliers e valores inválidos**
# a) Outliers em `amt` (Z-score acima de 3)
window_spec = Window.orderBy("amt")
df = df.withColumn("z_score", (col("amt") - mean(col("amt")).over(window_spec)) / stddev(col("amt")).over(window_spec))
outliers = df.filter((col("z_score") > 3) | (col("z_score") < -3)).count()
df = df.drop("z_score")
if outliers > 0:
    print(f"⚠️ {outliers} registros detectados como outliers em 'amt' e devem ser removidos!")

# b) Cidades fictícias (população menor que 100)
invalid_cities = df.filter(col("city_pop") < 100).count()
if invalid_cities > 0:
    print(f"⚠️ {invalid_cities} registros possuem 'city_pop' < 100 e devem ser removidos!")

# 5️⃣ **Detecção de possíveis fraudes**
# a) Transações acima de $10.000
high_value_frauds = df.filter(col("amt") > 10000).count()
if high_value_frauds > 0:
    print(f"🚨 {high_value_frauds} transações acima de $10.000 identificadas como possíveis fraudes!")

# b) Múltiplas transações no mesmo comerciante em menos de 10 segundos
window_spec = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")
df = df.withColumn("time_diff", unix_timestamp(col("trans_date_trans_time")) - lag(unix_timestamp(col("trans_date_trans_time"))).over(window_spec))
fast_transactions = df.filter(col("time_diff") < 10).count()
if fast_transactions > 0:
    print(f"🚨 {fast_transactions} casos de múltiplas transações rápidas identificadas!")

# c) Compras em estados diferentes no mesmo dia
df = df.withColumn("date", col("trans_date_trans_time").cast("date"))
multi_state_purchases = df.select("cc_num", "date", "state").distinct().groupBy("cc_num", "date").count().filter(col("count") > 1).count()
if multi_state_purchases > 0:
    print(f"🚨 {multi_state_purchases} cartões usados em estados diferentes no mesmo dia!")

# 6️⃣ **Verificar duplicatas**
duplicate_count = df.count() - df.dropDuplicates().count()
if duplicate_count > 0:
    print(f"⚠️ Existem {duplicate_count} registros duplicados no dataset!")
else:
    print("✅ Nenhuma duplicata encontrada.")

print("📊 Validação de dados concluída!")
spark.stop()
