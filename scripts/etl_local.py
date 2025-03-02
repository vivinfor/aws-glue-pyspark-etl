from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, when, date_format, unix_timestamp, lag, concat, lit, count
from pyspark.sql.window import Window
import os

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("ETL Local - Fraude Financeira") \
    .getOrCreate()

# Diretórios locais
INPUT_PATH = "data/dados-brutos.csv"
OUTPUT_PATH = "data/dados-processados/"

# Ler CSV com separador "|"
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True, sep="|")

# Remover duplicatas
df = df.dropDuplicates()

# Remover registros onde colunas críticas sejam nulas
df = df.na.drop(subset=["cc_num", "amt", "is_fraud"])

# Preencher valores nulos em colunas opcionais
df = df.fillna({
    "merchant": "Desconhecido",
    "city": "Não informado",
    "state": "Não informado",
    "lat": 0.0,
    "long": 0.0
})

# Definir uma janela para cálculo estatístico (particionada por categoria)
window_spec = Window.partitionBy("category").orderBy("amt")

# Calcular Z-score corretamente
df = df.withColumn("z_score", (col("amt") - mean(col("amt")).over(window_spec)) / stddev(col("amt")).over(window_spec))

# Filtrar outliers mantendo apenas valores dentro de 3 desvios padrão
df = df.filter(col("z_score").between(-3, 3)).drop("z_score")

# Criar coluna combinando data e hora
df = df.withColumn("trans_date_trans_time", concat(col("trans_date"), lit(" "), col("trans_time")))

# Converter para timestamp
df = df.withColumn("trans_date_trans_time", col("trans_date_trans_time").cast("timestamp"))

# Criar colunas de dia da semana e horário
df = df.withColumn("day_of_week", date_format(col("trans_date_trans_time"), "E"))
df = df.withColumn("hour_of_day", date_format(col("trans_date_trans_time"), "HH").cast("int"))

# Criar coluna categorizando o período da transação
df = df.withColumn(
    "transaction_period",
    when(col("hour_of_day") < 6, "Madrugada")
    .when(col("hour_of_day") < 12, "Manhã")
    .when(col("hour_of_day") < 18, "Tarde")
    .otherwise("Noite")
)

# Criar uma flag para transações acima de 10.000
df = df.withColumn("possible_fraud_high_value", (col("amt") > 10000).cast("integer"))

# Criar uma janela para verificar transações consecutivas do mesmo cartão no mesmo comerciante
window_spec = Window.partitionBy("cc_num", "merchant").orderBy("trans_date_trans_time")

# Calcular a diferença de tempo entre transações consecutivas
df = df.withColumn("time_diff", unix_timestamp("trans_date_trans_time") - lag(unix_timestamp("trans_date_trans_time")).over(window_spec))

# Criar uma flag para múltiplas transações em menos de 10 segundos
df = df.withColumn("possible_fraud_fast_transactions", (col("time_diff") < 10).cast("integer"))

# Ajuste de tipos de dados para conformidade com schema esperado
df = df.withColumn("cc_num", col("cc_num").cast("string"))
df = df.withColumn("amt", col("amt").cast("float"))
df = df.withColumn("zip", col("zip").cast("int"))
df = df.withColumn("lat", col("lat").cast("float"))
df = df.withColumn("long", col("long").cast("float"))
df = df.withColumn("city_pop", col("city_pop").cast("int"))
df = df.withColumn("dob", col("dob").cast("string"))
df = df.withColumn("unix_time", col("unix_time").cast("int"))
df = df.withColumn("merch_lat", col("merch_lat").cast("float"))
df = df.withColumn("merch_long", col("merch_long").cast("float"))
df = df.withColumn("is_fraud", col("is_fraud").cast("int"))

# Contagem de valores nulos para validação final
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()

# Criar diretório de saída se não existir
if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

# Contagem final de registros após processamento
print(f"Total de registros processados: {df.count()}")

# Salvar os dados processados em formato Parquet
df.write.mode("overwrite").partitionBy("category").parquet(OUTPUT_PATH)
print("ETL Finalizado com Sucesso!")

spark.stop()
