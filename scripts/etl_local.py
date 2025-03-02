from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, when, date_format, unix_timestamp, lag, concat, lit
from pyspark.sql.window import Window
from data_validation import validate_data  # Importar a função de validação

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("ETL Local - Fraude Financeira") \
    .getOrCreate()

# Caminhos dos arquivos
INPUT_PATH = "data/dados-brutos.csv"
OUTPUT_PATH = "data/dados-processados/"

# 📌 1️⃣ Extração
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True, sep="|")

# 📌 2️⃣ Validação Inicial
if not validate_data(df):
    print("❌ Falha na validação inicial dos dados. Corrija os problemas antes de continuar.")
    spark.stop()
    exit(1)

# 📌 3️⃣ Transformação dos Dados
df = df.dropDuplicates()
df = df.na.drop(subset=["cc_num", "amt", "is_fraud"])
df = df.fillna({"merchant": "Desconhecido", "city": "Não informado", "state": "Não informado", "lat": 0.0, "long": 0.0})

# Aplicação de Z-score para remoção de outliers
window_spec = Window.orderBy("amt")
df = df.withColumn("z_score", (col("amt") - mean("amt").over(window_spec)) / stddev("amt").over(window_spec))
df = df.filter(col("z_score").between(-3, 3)).drop("z_score")

# Criação de colunas derivadas
df = df.withColumn("trans_date_trans_time", concat(col("trans_date"), lit(" "), col("trans_time")).cast("timestamp"))
df = df.withColumn("day_of_week", date_format(col("trans_date_trans_time"), "E"))
df = df.withColumn("hour_of_day", date_format(col("trans_date_trans_time"), "HH").cast("int"))
df = df.withColumn(
    "transaction_period",
    when(col("hour_of_day") < 6, "Madrugada")
    .when(col("hour_of_day") < 12, "Manhã")
    .when(col("hour_of_day") < 18, "Tarde")
    .otherwise("Noite")
)
df = df.withColumn("possible_fraud_high_value", (col("amt") > 10000).cast("integer"))

# Verificação de transações repetidas em curto intervalo
window_spec = Window.partitionBy("cc_num", "merchant").orderBy("unix_time")
df = df.withColumn("time_diff", unix_timestamp(col("trans_date_trans_time")) - lag(unix_timestamp(col("trans_date_trans_time"))).over(window_spec))
df = df.withColumn("possible_fraud_fast_transactions", (col("time_diff") < 10).cast("integer"))

# 📌 4️⃣ Validação Final
if not validate_data(df):  
    print("❌ Erros identificados após a transformação. Corrija antes de salvar.")
    spark.stop()
    exit(1)

# 📌 5️⃣ Carga dos dados processados
df.write.mode("overwrite").parquet(OUTPUT_PATH)

print("✅ ETL Finalizado com Sucesso!")
spark.stop()
