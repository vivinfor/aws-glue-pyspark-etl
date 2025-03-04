import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("Query Optimization - PySpark vs SQL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Caminho do diretório processado
data_path = "data/processed"

# Verificar se os dados processados existem
if not os.path.exists(data_path):
    raise FileNotFoundError(f"❌ Diretório '{data_path}' não encontrado! Execute o ETL primeiro.")

# Carregar dados processados
print("📂 Carregando dados processados...")
df = spark.read.parquet(data_path)
print(f"✅ Total de registros carregados: {df.count()}")

# Criar View Temporária para SQL Queries
df.createOrReplaceTempView("transactions")

# Função para medir tempo de execução
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

# 🔹 4️⃣ Impacto do Particionamento
partitioned_df = df.repartition("category")
partitioned_df.createOrReplaceTempView("transactions_partitioned")
execute_query("""
    SELECT category, COUNT(*) as num_transacoes
    FROM transactions_partitioned
    GROUP BY category
""", "contagem por categoria (dados particionados)")

print("✅ Testes de performance concluídos!")

# Finalizar sessão Spark
spark.stop()
