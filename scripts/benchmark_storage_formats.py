import os
import time
import yaml
import shutil
import logging
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 📌 Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 📂 Carregar configuração do YAML
CONFIG_PATH = "config/config.yaml"
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

data_path = os.path.abspath(config["optimized_data_path"])
benchmark_output = config["benchmark_output"]
benchmark_options = config["benchmark_options"]

# 🚀 Criar sessão Spark
spark = SparkSession.builder.appName("Benchmark Storage Formats").getOrCreate()
logger.info("💻 Sessão Spark iniciada.")

# 📂 Carregar dados processados
logger.info("📂 Carregando dados processados para benchmark...")
df = spark.read.parquet(data_path)
total_records = df.count()
logger.info(f"✅ Total de registros carregados: {total_records}")

# 📌 Função para medir tempo de escrita

def measure_write_time(df, format_name, output_path, partition_col=None):
    if os.path.exists(output_path):
        shutil.rmtree(output_path)  # Remover diretório anterior
    
    start_time = time.time()
    if partition_col:
        df.write.mode("overwrite").partitionBy(partition_col).format(format_name).save(output_path)
    else:
        df.write.mode("overwrite").format(format_name).save(output_path)
    elapsed_time = time.time() - start_time
    
    return elapsed_time

# 📌 Função para medir tempo de leitura

def measure_read_time(spark, format_name, input_path):
    start_time = time.time()
    df_read = spark.read.format(format_name).load(input_path)
    df_read.count()  # Aciona a leitura
    elapsed_time = time.time() - start_time
    return elapsed_time

# 📌 Benchmark dos formatos
benchmark_results = {}

for format_name, path in benchmark_output.items():
    logger.info(f"🚀 Testando formato: {format_name.upper()}")
    benchmark_results[format_name] = {}
    
    if benchmark_options["test_write"]:
        write_time = measure_write_time(df, format_name, path)
        benchmark_results[format_name]["write_time"] = round(write_time, 3)
        logger.info(f"✅ Tempo de escrita ({format_name}): {write_time:.3f} segundos")
    
    if benchmark_options["test_read"]:
        read_time = measure_read_time(spark, format_name, path)
        benchmark_results[format_name]["read_time"] = round(read_time, 3)
        logger.info(f"✅ Tempo de leitura ({format_name}): {read_time:.3f} segundos")
    
    if benchmark_options["measure_size"]:
        total_size = sum(os.path.getsize(os.path.join(root, file)) for root, _, files in os.walk(path) for file in files)
        benchmark_results[format_name]["size_mb"] = round(total_size / (1024 * 1024), 2)
        logger.info(f"✅ Tamanho do arquivo ({format_name}): {total_size / (1024 * 1024):.2f} MB")

# 📂 **Salvar relatório consolidado em JSON**
benchmark_report_path = "data/benchmark/benchmark_results.json"
os.makedirs("data/benchmark", exist_ok=True)

import json
with open(benchmark_report_path, "w", encoding="utf-8") as f:
    json.dump(benchmark_results, f, indent=4)
logger.info(f"📂 Benchmark salvo: {benchmark_report_path}")

logger.info("🚀 Benchmark concluído!")
spark.stop()
