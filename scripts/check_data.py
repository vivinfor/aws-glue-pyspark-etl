import os
import pandas as pd

# Diretório onde os arquivos CSV estão armazenados
DATA_DIR = "data/"

# Lista de arquivos que queremos checar
files_to_check = ["treino.csv", "teste.csv"]

# Função para verificar se o arquivo contém a coluna 'is_fraud'
def find_raw_data():
    for file in files_to_check:
        file_path = os.path.join(DATA_DIR, file)
        
        if os.path.exists(file_path):
            try:
                # Ler apenas as primeiras 5 linhas para otimizar a leitura
                df = pd.read_csv(file_path, sep="|", nrows=5)
                
                if "is_fraud" in df.columns:
                    print(f"✅ O arquivo '{file}' contém a coluna 'is_fraud' e será usado como 'dados-brutos.csv'.")
                    
                    # Renomeia o arquivo correto para 'dados-brutos.csv'
                    new_path = os.path.join(DATA_DIR, "dados-brutos.csv")
                    os.rename(file_path, new_path)
                    
                    print(f"✅ '{file}' foi renomeado para 'dados-brutos.csv'.")
                    return new_path
                else:
                    print(f"⚠️ O arquivo '{file}' NÃO contém a coluna 'is_fraud'.")
            except Exception as e:
                print(f"❌ Erro ao processar '{file}': {e}")

    print("❌ Nenhum arquivo válido encontrado com a coluna 'is_fraud'.")
    return None

# Executar a verificação
if __name__ == "__main__":
    result = find_raw_data()

    if result:
        print(f"\n✅ Agora execute o ETL com:\npython scripts/etl_local.py")
    else:
        print("\n⚠️ Nenhum arquivo adequado foi encontrado. Verifique os dados.")
