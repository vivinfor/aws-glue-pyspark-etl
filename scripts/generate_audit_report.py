import os
import json
import yaml
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, mean, stddev, unix_timestamp, lag, count, year, month, sum as spark_sum
)
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

df = df.withColumn("date", col("trans_date_trans_time").cast("date"))
multi_state_purchases = df.groupBy("cc_num", "date").agg(count("state").alias("state_count")).filter(col("state_count") > 1)

# 📌 Criar pasta de auditoria se não existir
if not os.path.exists(AUDIT_PATH):
    os.makedirs(AUDIT_PATH)

# 📂 **Salvar registros inválidos**
if invalid_records.count() > 0:
    invalid_records.toPandas().to_csv(os.path.join(AUDIT_PATH, "invalid_records.csv"), index=False, encoding="utf-8")
    print(f"📂 Registros inválidos salvos para auditoria: {os.path.join(AUDIT_PATH, 'invalid_records.csv')}")

# 📂 **Salvar outliers**
if outliers.count() > 0:
    outliers.toPandas().to_csv(os.path.join(AUDIT_PATH, "outliers.csv"), index=False, encoding="utf-8")
    print(f"📂 Outliers salvos para auditoria: {os.path.join(AUDIT_PATH, 'outliers.csv')}")

# 📂 **Salvar fraudes suspeitas**
if high_value_frauds.count() > 0:
    high_value_frauds.toPandas().to_csv(os.path.join(AUDIT_PATH, "high_value_frauds.csv"), index=False, encoding="utf-8")
    print(f"📂 Transações de alto valor salvas para auditoria: {os.path.join(AUDIT_PATH, 'high_value_frauds.csv')}")

if fast_transactions.count() > 0:
    fast_transactions.toPandas().to_csv(os.path.join(AUDIT_PATH, "fast_transactions.csv"), index=False, encoding="utf-8")
    print(f"📂 Transações rápidas suspeitas salvas para auditoria: {os.path.join(AUDIT_PATH, 'fast_transactions.csv')}")

if multi_state_purchases.count() > 0:
    multi_state_purchases.toPandas().to_csv(os.path.join(AUDIT_PATH, "multi_state_purchases.csv"), index=False, encoding="utf-8")
    print(f"📂 Cartões usados em múltiplos estados no mesmo dia salvos para auditoria: {os.path.join(AUDIT_PATH, 'multi_state_purchases.csv')}")

# 📊 **Total de transações e fraudes em 2023**
df_2023 = df.filter(year(col("trans_date_trans_time")) == 2023)
total_transactions_2023 = df_2023.count()
total_frauds_2023 = df_2023.filter(col("is_fraud") == 1).count()

# 📊 **Fraudes por mês em 2023**
fraud_by_month_2023 = (
    df_2023.filter(col("is_fraud") == 1)
    .groupBy(month(col("trans_date_trans_time")).alias("month"))
    .count()
    .orderBy("month")
)
fraud_by_month_df = fraud_by_month_2023.toPandas()
fraud_by_month_df.rename(columns={"count": "total_frauds"}, inplace=True)
fraud_by_month_df.to_csv(os.path.join(AUDIT_PATH, "fraud_by_month_2023.csv"), index=False, encoding="utf-8")

# 📊 **Verificação de transações duplicadas (`cc_num`, `trans_num`)**
duplicates = df.groupBy("cc_num", "trans_num").count().filter(col("count") > 1)
if duplicates.count() > 0:
    duplicates.toPandas().to_csv(os.path.join(AUDIT_PATH, "duplicate_transactions.csv"), index=False, encoding="utf-8")
    print(f"📂 Transações duplicadas salvas para auditoria: {os.path.join(AUDIT_PATH, 'duplicate_transactions.csv')}")

# 📊 **Comparação de valores totais `amt`**
total_amt = df.select(spark_sum("amt")).collect()[0][0]
total_fraud_amt = df.filter(col("is_fraud") == 1).select(spark_sum("amt")).collect()[0][0]

# 📊 **Gerar gráfico de fraudes por mês**
plt.figure(figsize=(10, 5))
plt.bar(fraud_by_month_df["month"], fraud_by_month_df["total_frauds"], color="red")
plt.xlabel("Mês")
plt.ylabel("Total de Fraudes")
plt.title("Evolução das Fraudes por Mês - 2023")
plt.xticks(range(1, 13))
plt.grid()
plt.savefig(os.path.join(AUDIT_PATH, "fraud_by_month_2023.png"))
plt.close()

print(f"📊 Evolução das fraudes por mês salva como gráfico.")

print(f"📊 Total de transações: {total_amt:.2f}")
print(f"📊 Total de fraudes em valor: {total_fraud_amt:.2f}")

print("\n🚀 Relatório de auditoria concluído!")

# 🛑 Encerrar Spark
spark.stop()
