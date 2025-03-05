import os
import yaml
import pandas as pd
import pyarrow.parquet as pq

# 📌 Carregar Configuração do YAML
config_path = os.path.abspath("config/config.yaml")
if not os.path.exists(config_path):
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# 📂 Caminho dos dados otimizados
OPTIMIZED_DATA_DIR = os.path.normpath(config.get("optimized_data_path", "data/optimized/"))

if not os.path.exists(OPTIMIZED_DATA_DIR):
    raise FileNotFoundError(f"❌ O diretório '{OPTIMIZED_DATA_DIR}' não foi encontrado!")

# 📂 Listar arquivos Parquet
parquet_files = [f for f in os.listdir(OPTIMIZED_DATA_DIR) if f.endswith(".parquet")]

if not parquet_files:
    raise FileNotFoundError(f"❌ Nenhum arquivo Parquet encontrado em '{OPTIMIZED_DATA_DIR}'.")

print(f"✅ Arquivos Parquet encontrados: {parquet_files}")

# 📌 Carregar e verificar os dados
df_list = []
for file in parquet_files:
    file_path = os.path.join(OPTIMIZED_DATA_DIR, file)
    print(f"📂 Lendo: {file_path}")
    table = pq.read_table(file_path)
    df = table.to_pandas()
    df_list.append(df)

# 📌 Concatenar todos os arquivos Parquet
df_final = pd.concat(df_list, ignore_index=True)

# 📊 Exibir estatísticas
print("📊 Amostra dos dados:")
print(df_final.head())

print("\n📊 Estatísticas básicas:")
print(df_final.describe())

print("\n📝 Tipos de dados:")
print(df_final.dtypes)

# 🚀 Salvar uma amostra como CSV para teste no Power BI (opcional)
sample_csv_path = os.path.join(OPTIMIZED_DATA_DIR, "sample_powerbi.csv")
df_final.to_csv(sample_csv_path, index=False)
print(f"📂 Amostra salva para teste: {sample_csv_path}")
