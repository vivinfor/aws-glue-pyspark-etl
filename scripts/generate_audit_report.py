import os
import json
import yaml
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, unix_timestamp, lag, count
from pyspark.sql.window import Window

# 📌 Carregar configurações
CONFIG_PATH = "config/config.yaml"
SCHEMA_PATH = "config/schema.json"

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")
if not os.path.exists(SCHEMA_PATH):
    raise FileNotFoundError("❌ Arquivo 'schema.json' não encontrado!")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

with open(SCHEMA_PATH, "r") as f:
    schema = json.load(f)

DATA_PATH = os.path.abspath(config["data_path"])
AUDIT_PATH = os.path.join(DATA_PATH, "audit_report")

# 🚀 Criar sessão Spark
spark = SparkSession.builder.appName("Audit Report").getOrCreate()

# 📂 Carregar dados processados
df = spark.read.parquet(DATA_PATH)

# 🛑 **Filtrar registros inválidos**
invalid_records = df.filter(col("amt").isNull() | col("is_fraud").isNull())

# ⚠️ **Detectar outliers em `amt`**
amt_stats = df.select(mean("amt").alias("mean_amt"), stddev("amt").alias("std_amt")).collect()[0]
mean_amt, std_amt = amt_stats["mean_amt"], amt_stats["std_amt"]

outliers = df.filter((col("amt") > mean_amt + 3 * std_amt) | (col("amt") < mean_amt - 3 * std_amt))

# 🚨 **Detectar possíveis fraudes**
high_value_frauds = df.filter(col("amt") > 10000)
window_spec = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")
df = df.withColumn("time_diff", unix_timestamp(col("trans_date_trans_time")) - lag(unix_timestamp(col("trans_date_trans_time"))).over(window_spec))
fast_transactions = df.filter(col("time_diff") < 10)
multi_state_purchases = df.groupBy("cc_num", "date").agg(count("state").alias("state_count")).filter(col("state_count") > 1)

# 📌 Criar pasta de auditoria se não existir
if not os.path.exists(AUDIT_PATH):
    os.makedirs(AUDIT_PATH)

# 📂 **Salvar registros inválidos**
if invalid_records.count() > 0:
    invalid_records.toPandas().to_csv(os.path.join(AUDIT_PATH, "invalid_records.csv"), index=False)
    print(f"📂 Registros inválidos salvos para auditoria: {os.path.join(AUDIT_PATH, 'invalid_records.csv')}")

# 📂 **Salvar outliers**
if outliers.count() > 0:
    outliers.toPandas().to_csv(os.path.join(AUDIT_PATH, "outliers.csv"), index=False)
    print(f"📂 Outliers salvos para auditoria: {os.path.join(AUDIT_PATH, 'outliers.csv')}")

# 📂 **Salvar fraudes suspeitas**
if high_value_frauds.count() > 0:
    high_value_frauds.toPandas().to_csv(os.path.join(AUDIT_PATH, "high_value_frauds.csv"), index=False)
    print(f"📂 Transações de alto valor salvas para auditoria: {os.path.join(AUDIT_PATH, 'high_value_frauds.csv')}")

if fast_transactions.count() > 0:
    fast_transactions.toPandas().to_csv(os.path.join(AUDIT_PATH, "fast_transactions.csv"), index=False)
    print(f"📂 Transações rápidas suspeitas salvas para auditoria: {os.path.join(AUDIT_PATH, 'fast_transactions.csv')}")

if multi_state_purchases.count() > 0:
    multi_state_purchases.toPandas().to_csv(os.path.join(AUDIT_PATH, "multi_state_purchases.csv"), index=False)
    print(f"📂 Cartões usados em múltiplos estados no mesmo dia salvos para auditoria: {os.path.join(AUDIT_PATH, 'multi_state_purchases.csv')}")

# 📊 **Gerar resumo das validações**
summary_data = {
    "Registros inválidos (`amt` nulo)": [invalid_records.count()],
    "Registros inválidos (`is_fraud` nulo)": [invalid_records.count()],
    "Registros outliers em `amt`": [outliers.count()],
    "Transações acima de $10.000": [high_value_frauds.count()],
    "Transações rápidas (< 10s)": [fast_transactions.count()],
    "Cartões usados em múltiplos estados no mesmo dia": [multi_state_purchases.count()]
}

summary_df = pd.DataFrame.from_dict(summary_data, orient="index", columns=["Total"])
summary_csv_path = os.path.join(AUDIT_PATH, "validation_summary.csv")
summary_df.to_csv(summary_csv_path)

print(f"📊 Resumo das validações salvo: {summary_csv_path}")

print("\n🚀 Relatório de auditoria concluído!")

# 🛑 Encerrar Spark
spark.stop()
