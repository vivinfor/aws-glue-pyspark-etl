import pandas as pd

# Caminho do arquivo Parquet local
file_path = r"C:\Users\vivin\aws-glue-pyspark-etl\data\optimized\category=shopping_pos\part-00000-xxxxxxxx.snappy.parquet"

# Carregar e exibir os dados
df = pd.read_parquet(file_path)

# Exibir as 5 primeiras linhas
print(df.head())

# Mostrar informações das colunas
print(df.info())

df.to_csv("dados_powerbi.csv", index=False)
