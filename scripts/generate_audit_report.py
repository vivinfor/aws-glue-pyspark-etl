import os
import json
import yaml
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, mean, stddev, unix_timestamp, lag, count, year, month, sum as spark_sum, when
)
from pyspark.sql.window import Window

# 📌 Carregar configurações
CONFIG_PATH = "config/config.yaml"
SCHEMA_PATH = "config/schema.json"
AUDIT_PATH = "data/audit_reports"

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")
if not os.path.exists(SCHEMA_PATH):
    raise FileNotFoundError("❌ Arquivo 'schema.json' não encontrado!")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

data_path = os.path.abspath(config["optimized_data_path"])

# 🚀 Criar sessão Spark
spark = SparkSession.builder.appName("Audit Report").getOrCreate()

# 📂 Carregar dados processados
df = spark.read.parquet(data_path)

total_records = df.count()

# 🛑 **Filtrar registros inválidos**
invalid_records = df.filter(col("amt").isNull() | col("is_fraud").isNull()).count()

# ⚠️ **Detectar outliers em `amt`**
amt_stats = df.select(mean("amt").alias("mean_amt"), stddev("amt").alias("std_amt")).collect()[0]
mean_amt, std_amt = amt_stats["mean_amt"], amt_stats["std_amt"]
outliers = df.filter((col("amt") > mean_amt + 3 * std_amt) | (col("amt") < mean_amt - 3 * std_amt)).count()

# 🚨 **Detectar possíveis fraudes**
high_value_frauds = df.filter(col("amt") > 10000).count()
window_spec = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")
df = df.withColumn("time_diff", unix_timestamp(col("trans_date_trans_time")) - lag(unix_timestamp(col("trans_date_trans_time"))).over(window_spec))
fast_transactions = df.filter(col("time_diff") < 10).count()
total_fraud_cases = df.filter(col("is_fraud") == 1).count()
fraud_by_category = df.filter(col("is_fraud") == 1)
    .groupBy("category").count().orderBy(col("count").desc())
    .toPandas().set_index("category")["count"].to_dict()

# 📊 **Resumo financeiro**
total_transaction_amount = df.select(spark_sum("amt")).collect()[0][0]
total_fraud_amount = df.filter(col("is_fraud") == 1).select(spark_sum("amt")).collect()[0][0]
average_fraud_value = df.filter(col("is_fraud") == 1).select(mean("amt")).collect()[0][0]
average_transaction_value = df.select(mean("amt")).collect()[0][0]

# 📂 **Criar diretório de auditoria se não existir**
os.makedirs(AUDIT_PATH, exist_ok=True)

# 📂 **Salvar relatório consolidado em JSON**
auditoria = {
    "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "etl_version": "1.0.0",
    "source": config.get("environment", "unknown"),
    "total_records": total_records,
    "invalid_records": invalid_records,
    "outliers_detected": {"total": outliers, "percentage": round((outliers / total_records) * 100, 2)},
    "fraud_analysis": {
        "high_value_frauds": high_value_frauds,
        "fast_transactions": fast_transactions,
        "total_fraud_cases": total_fraud_cases,
        "fraud_by_category": fraud_by_category
    },
    "transaction_amounts": {
        "total_transaction_amount": total_transaction_amount,
        "total_fraud_amount": total_fraud_amount,
        "average_fraud_value": average_fraud_value,
        "average_transaction_value": average_transaction_value
    }
}

audit_json_path = os.path.join(AUDIT_PATH, "audit_summary.json")
with open(audit_json_path, "w", encoding="utf-8") as f:
    json.dump(auditoria, f, indent=4)
print(f"📂 Relatório de auditoria salvo: {audit_json_path}")

# 📊 **Gerar gráfico de fraudes por mês**
df_2023 = df.filter(year(col("trans_date_trans_time")) == 2023)
fraud_by_month_2023 = (
    df_2023.filter(col("is_fraud") == 1)
    .groupBy(month(col("trans_date_trans_time")).alias("month"))
    .count()
    .orderBy("month")
)
fraud_by_month_df = fraud_by_month_2023.toPandas()
fraud_by_month_df.rename(columns={"count": "total_frauds"}, inplace=True)

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

# 🚀 Relatório finalizado
print("\n✅ Relatório de auditoria concluído com sucesso!")

# 🛑 Encerrar Spark
spark.stop()
