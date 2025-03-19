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

# 📌 Ler todos os Parquets e consolidar no Pandas
df_list = []
missing_columns = set()

for file in parquet_files:
    try:
        df_temp = pq.read_table(file).to_pandas()
        
        # Se a coluna `category` estiver ausente, adicioná-la com valores NaN
        if "category" not in df_temp.columns:
            missing_columns.add(file)
            df_temp["category"] = pd.NA

        df_list.append(df_temp)
    except Exception as e:
        print(f"⚠️ Erro ao ler {file}: {e}")

# Se nenhum arquivo válido foi lido, interromper
if not df_list:
    raise RuntimeError("❌ Nenhum arquivo Parquet válido foi carregado!")

df = pd.concat(df_list, ignore_index=True)

# Exibir estatísticas básicas
print(df.head())
print(df.describe())

# 📌 **Verificar colunas ausentes**
if missing_columns:
    print(f"⚠️ Os seguintes arquivos estavam sem a coluna `category` e foram corrigidos dinamicamente:\n{missing_columns}")

# 📂 Definir caminho para o CSV final
CSV_OUTPUT_PATH = os.path.abspath("data/powerbi_data.csv")

# 🚀 Salvar como CSV
df.to_csv(CSV_OUTPUT_PATH, index=False, encoding="utf-8")
print(f"✅ Arquivo CSV salvo em: {CSV_OUTPUT_PATH}")

# 🚀 **Comparação entre CSV e Parquet**
df_csv = pd.read_csv(CSV_OUTPUT_PATH)

# Comparação de totais
if len(df) == len(df_csv):
    print("✅ O número de registros no CSV é igual ao do Parquet.")
else:
    print(f"⚠️ Divergência detectada! CSV tem {len(df_csv)} registros, Parquet tem {len(df)}.")

# Comparação de soma dos valores `amt`
if df["amt"].sum() == df_csv["amt"].sum():
    print("✅ A soma dos valores `amt` é consistente entre Parquet e CSV.")
else:
    print("⚠️ A soma dos valores `amt` diverge entre Parquet e CSV!")

print("🚀 Processo concluído!")
