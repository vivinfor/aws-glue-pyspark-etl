import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min

# 📌 Criar sessão Spark
spark = SparkSession.builder \
    .appName("Query Optimization - PySpark vs SQL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# 📂 Caminho do diretório processado
data_path = "data/processed"

# 📌 Verificar se os dados processados existem
if not os.path.exists(data_path):
    raise FileNotFoundError(f"❌ Diretório '{data_path}' não encontrado! Execute o ETL primeiro.")

# 📂 Carregar dados processados
print("📂 Carregando dados processados...")
df = spark.read.parquet(data_path)
print(f"✅ Total de registros carregados: {df.count()}")

# 📌 Criar View Temporária para SQL Queries
df.createOrReplaceTempView("transactions")

# 📌 Garantir que as colunas esperadas estão disponíveis
expected_columns = {
    "trans_date_trans_time", "cc_num", "merchant", "category", "amt",
    "first", "last", "gender", "street", "city", "state", "zip",
    "lat", "long", "city_pop", "job", "dob", "trans_num", "unix_time",
    "merch_lat", "merch_long", "is_fraud", "day_of_week", "hour_of_day",
    "transaction_period", "possible_fraud_high_value", "possible_fraud_fast_transactions"
}
actual_columns = set(df.columns)

# 🔥 Verificar colunas inesperadas
extra_columns = actual_columns - expected_columns
if extra_columns:
    print(f"⚠️ Removendo colunas extras: {extra_columns}")
    df = df.drop(*extra_columns)

# 🔥 Verificar colunas faltantes
missing_columns = expected_columns - actual_columns
if missing_columns:
    raise ValueError(f"❌ Colunas faltantes no dataset processado: {missing_columns}")

# 📌 Criar View Temporária novamente após ajuste
df.createOrReplaceTempView("transactions")

# 📌 Função para medir tempo de execução de queries
def execute_query(query, description):
    start_time = time.time()
    result = spark.sql(query)
    result.show(5)
    elapsed_time = time.time() - start_time
    print(f"⏳ Tempo para {description}: {elapsed_time:.4f} segundos")
    return result

# 🔹 1️⃣ Consulta Básica: Contagem de Transações
execute_query("""
    SELECT COUNT(*) as total_transacoes
    FROM transactions
""", "contar total de transações")

# 🔹 2️⃣ Agregação por Categoria
execute_query("""
    SELECT category, COUNT(*) as num_transacoes, AVG(amt) as avg_value
    FROM transactions
    GROUP BY category
    ORDER BY num_transacoes DESC
""", "agregação por categoria")

# 🔹 3️⃣ Transações com Maior Valor por Categoria
execute_query("""
    SELECT category, MAX(amt) as max_value
    FROM transactions
    GROUP BY category
    ORDER BY max_value DESC
""", "transações de maior valor")

# 🔹 4️⃣ Contagem de fraudes por período do dia
execute_query("""
    SELECT transaction_period, COUNT(*) as num_fraudes
    FROM transactions
    WHERE is_fraud = 1
    GROUP BY transaction_period
    ORDER BY num_fraudes DESC
""", "contagem de fraudes por período do dia")

# 🔹 5️⃣ Impacto do Particionamento
partitioned_df = df.repartition("category")
partitioned_df.createOrReplaceTempView("transactions_partitioned")
execute_query("""
    SELECT category, COUNT(*) as num_transacoes
    FROM transactions_partitioned
    GROUP BY category
""", "contagem por categoria (dados particionados)")

print("✅ Testes de performance concluídos!")

# 📌 Finalizar sessão Spark
spark.stop()
