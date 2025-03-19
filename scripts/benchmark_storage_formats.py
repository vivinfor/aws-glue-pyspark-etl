import os
import time
import json
import yaml
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# 📌 Carregar configurações
CONFIG_PATH = "config/config.yaml"

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

data_path = os.path.abspath(config["optimized_data_path"])
benchmark_path = "data/benchmark"
os.makedirs(benchmark_path, exist_ok=True)

# 🚀 Criar sessão Spark
spark = SparkSession.builder \
    .appName("Benchmark Storage Formats") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 📂 Carregar dados processados
df = spark.read.parquet(data_path)

# 📊 **Benchmark de formatos de armazenamento**
formats = ["csv", "parquet", "delta"]
results = []

for fmt in formats:
    format_path = os.path.join(benchmark_path, fmt)
    os.makedirs(format_path, exist_ok=True)
    
    # 📝 Medir tempo de escrita
    start_time = time.time()
    if fmt == "csv":
        df.write.mode("overwrite").option("header", True).csv(format_path)
    elif fmt == "parquet":
        df.write.mode("overwrite").parquet(format_path)
    elif fmt == "delta":
        df.write.mode("overwrite").format("delta").save(format_path)
    write_time = time.time() - start_time
    
    # 📂 Medir tamanho do diretório
    total_size = sum(os.path.getsize(os.path.join(root, f)) for root, _, files in os.walk(format_path) for f in files) / (1024 * 1024)
    
    # 📖 Medir tempo de leitura
    start_time = time.time()
    if fmt == "csv":
        df_test = spark.read.option("header", True).csv(format_path)
    elif fmt == "parquet":
        df_test = spark.read.parquet(format_path)
    elif fmt == "delta":
        df_test = spark.read.format("delta").load(format_path)
    read_time = time.time() - start_time
    
    # 📊 Armazenar resultados
    results.append({
        "format": fmt,
        "write_time": write_time,
        "read_time": read_time,
        "size_mb": total_size
    })

# 📂 Salvar relatório em JSON
benchmark_json_path = os.path.join(benchmark_path, "benchmark_results.json")
with open(benchmark_json_path, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4)
print(f"📂 Relatório de benchmark salvo: {benchmark_json_path}")

# 📊 **Gerar gráficos comparativos**
formats = [res["format"] for res in results]
write_times = [res["write_time"] for res in results]
read_times = [res["read_time"] for res in results]
sizes = [res["size_mb"] for res in results]

plt.figure(figsize=(12, 6))

# 🚀 Tempo de escrita
plt.subplot(1, 3, 1)
plt.bar(formats, write_times, color=["blue", "green", "red"])
plt.xlabel("Formato")
plt.ylabel("Tempo (s)")
plt.title("Tempo de Escrita por Formato")

# 🚀 Tempo de leitura
plt.subplot(1, 3, 2)
plt.bar(formats, read_times, color=["blue", "green", "red"])
plt.xlabel("Formato")
plt.ylabel("Tempo (s)")
plt.title("Tempo de Leitura por Formato")

# 🚀 Tamanho do arquivo gerado
plt.subplot(1, 3, 3)
plt.bar(formats, sizes, color=["blue", "green", "red"])
plt.xlabel("Formato")
plt.ylabel("Tamanho (MB)")
plt.title("Tamanho do Arquivo por Formato")

plt.tight_layout()
plt.savefig(os.path.join(benchmark_path, "benchmark_comparison.png"))
plt.close()
print(f"📊 Gráficos de benchmark salvos como imagem.")

# 🚀 Finalização
print("✅ Benchmark concluído com sucesso!")
spark.stop()
