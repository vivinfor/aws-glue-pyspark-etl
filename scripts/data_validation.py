import os
import json
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, mean, stddev, unix_timestamp, lag, count, date_format, when
)
from pyspark.sql.window import Window

# 📌 Carregar configuração do YAML
CONFIG_PATH = "config/config.yaml"
SCHEMA_PATH = "config/schema.json"

# 🔍 Verificar se os arquivos existem
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")

if not os.path.exists(SCHEMA_PATH):
    raise FileNotFoundError("❌ Arquivo 'schema.json' não encontrado!")

# 📂 Carregar config.yaml
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

# 📂 Carregar schema.json
with open(SCHEMA_PATH, "r") as f:
    schema = json.load(f)

# 📌 Determinar ambiente e definir caminhos corretamente
IS_AWS = config.get("environment") == "aws"
DATA_PATH = config["aws_s3_output"] if IS_AWS else os.path.abspath(config["data_path"])

print(f"📂 Diretório de dados processados: {DATA_PATH}")

# 📌 Criar sessão Spark
spark = SparkSession.builder.appName("Validação de Dados - ETL").getOrCreate()

# 📂 Carregar os dados processados
df = spark.read.parquet(DATA_PATH)

# 🔍 **Verificar Schema**
expected_schema = {field["name"]: field["type"] for field in schema["fields"]}
missing_columns = [col for col in expected_schema.keys() if col not in df.columns]

if missing_columns:
    print(f"⚠️ Colunas ausentes no dataset: {missing_columns}")
else:
    print("✅ Todas as colunas esperadas estão presentes.")

# 🔍 **Validar tipos de dados**
for field in schema["fields"]:
    col_name = field["name"]
    expected_type = field["type"]

    if col_name in df.columns:
        actual_type = df.select(col_name).schema[0].dataType.simpleString()
        if expected_type == "double":
            expected_type = "float"  # Ajuste para compatibilidade
        if actual_type != expected_type:
            print(f"⚠️ Tipo incorreto na coluna '{col_name}': Esperado {expected_type}, encontrado {actual_type}")

# 🔍 **Verificação de `transaction_period`**
print("\n🔍 Verificando distribuição de `transaction_period`:")
df.groupBy("transaction_period").count().show()

print("\n🔍 Verificando `hour_of_day` e `transaction_period` juntos:")
df.select("hour_of_day", "transaction_period").groupby("hour_of_day", "transaction_period").count().orderBy("hour_of_day").show(24, False)

# 🔍 **Validar valores nulos críticos**
critical_nulls = {
    "cc_num": "Obrigatório",
    "amt": "Obrigatório",
    "is_fraud": "Obrigatório",
}

for col_name, rule in critical_nulls.items():
    if col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"🚨 ERRO: {null_count} registros possuem '{col_name}' nulo, registros devem ser descartados!")

# 🔍 **Detectar Outliers em `amt`**
amt_stats = df.select(mean("amt").alias("mean_amt"), stddev("amt").alias("std_amt")).collect()[0]
mean_amt, std_amt = amt_stats["mean_amt"], amt_stats["std_amt"]

outliers = df.filter((col("amt") > mean_amt + 3 * std_amt) | (col("amt") < mean_amt - 3 * std_amt)).count()
if outliers > 0:
    print(f"⚠️ {outliers} registros detectados como outliers em 'amt' e devem ser removidos!")

# 🔍 **Validar fraudes**
high_value_frauds = df.filter(col("amt") > 10000).count()
if high_value_frauds > 0:
    print(f"🚨 {high_value_frauds} transações acima de $10.000 identificadas como possíveis fraudes!")

# 🔍 **Validar transações rápidas no mesmo comerciante**
window_spec = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")
df = df.withColumn("time_diff", unix_timestamp(col("trans_date_trans_time")) - lag(unix_timestamp(col("trans_date_trans_time"))).over(window_spec))

fast_transactions = df.filter(col("time_diff") < 10).count()
if fast_transactions > 0:
    print(f"🚨 {fast_transactions} casos de múltiplas transações rápidas identificadas!")

# 🔍 **Detectar compras em múltiplos estados no mesmo dia**
df = df.withColumn("date", col("trans_date_trans_time").cast("date"))
multi_state_purchases = df.groupBy("cc_num", "date").agg(count("state").alias("state_count")).filter(col("state_count") > 1).count()

if multi_state_purchases > 0:
    print(f"🚨 {multi_state_purchases} cartões usados em estados diferentes no mesmo dia!")

# 🔍 **Verificar duplicatas**
duplicate_count = df.count() - df.dropDuplicates().count()
if duplicate_count > 0:
    print(f"⚠️ Existem {duplicate_count} registros duplicados no dataset!")
else:
    print("✅ Nenhuma duplicata encontrada.")

print("\n📊 Validação de dados concluída!")
spark.stop()
