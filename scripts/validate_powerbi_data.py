import os
import yaml
import glob
import pandas as pd
import pyarrow.parquet as pq

# 📂 Carregar Configuração do YAML
config_path = os.path.abspath("config/config.yaml")

if not os.path.exists(config_path):
    raise FileNotFoundError("❌ Arquivo 'config.yaml' não encontrado!")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# 📂 Definir diretório dos dados otimizados
OPTIMIZED_DATA_DIR = os.path.abspath(config.get("optimized_data_path", "data/optimized/"))

# 📂 Buscar arquivos Parquet (incluindo subdiretórios)
parquet_files = glob.glob(os.path.join(OPTIMIZED_DATA_DIR, "**", "*.parquet"), recursive=True)

# 🚀 Verificar se encontrou arquivos Parquet
if not parquet_files:
    raise FileNotFoundError(f"❌ Nenhum arquivo Parquet encontrado em '{OPTIMIZED_DATA_DIR}'.")

print(f"✅ {len(parquet_files)} arquivos Parquet encontrados.")

# 📌 Carregar os arquivos Parquet no Pandas
df_list = [pq.read_table(file).to_pandas() for file in parquet_files]
df = pd.concat(df_list, ignore_index=True)

# 📊 Exibir estatísticas básicas
print(df.head())
print(df.describe())
print(df.dtypes)

print("🚀 Validação concluída!")
