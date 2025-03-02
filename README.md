# AWS Glue & PySpark ETL

## 📌 Objetivo
Este projeto demonstra um pipeline de **ETL escalável** usando **AWS Glue e PySpark** para processamento e transformação de dados. Ele inclui técnicas para **limpeza, tratamento de outliers, estatísticas descritivas e armazenamento otimizado no S3**.

## 🔹 Fonte dos Dados
Os dados utilizados neste projeto são provenientes de um dataset público disponibilizado pelo Kaggle, referente a **transações financeiras sintéticas**. O dataset foi gerado através da ferramenta **Sparkov Data Generation**, simulando transações de janeiro a dezembro de 2023. 

### 📌 Link para o dataset:
[Kaggle - Fraude em Transações de Cartão de Crédito](https://www.kaggle.com/competitions/fraude-em-transaes-de-carto-de-crdito/data)

O dataset contém informações como:
- **Dados da transação** (data, valor, comerciante, categoria, localização, etc.).
- **Informações do titular do cartão** (nome, endereço, gênero, profissão, etc.).
- **Indicador de fraude (`is_fraud`)** para identificar transações fraudulentas.

## 🔹 Tecnologias Utilizadas
- **AWS Glue**: Para processamento e transformação de dados em grande escala.
- **PySpark**: Para manipulação e transformação eficiente dos dados.
- **AWS S3**: Para armazenamento dos dados brutos e processados.
- **SQL**: Para análise e validação de qualidade dos dados.

## 🚀 Pipeline de ETL
1. **Extração**: Carregamento do dataset público armazenado no **S3**.
2. **Transformação**:
   - Remoção de **valores nulos e duplicados**.
   - **Validação da estrutura do dataset** de acordo com um schema predefinido (`schema.json`).
   - **Detecção de outliers** usando **Z-score**.
   - Cálculo de **estatísticas descritivas** (média, mediana, desvio padrão, distribuição de valores faltantes).
   - Normalização de colunas.
3. **Carga**: Salvamento do dataset transformado no S3, pronto para análise posterior.

## 🔹 Validação dos Dados
Durante o pipeline ETL, aplicamos uma série de validações para garantir a qualidade e consistência dos dados:
- **Estrutura e tipos de dados**: Validação contra um schema predefinido (`schema.json`).
- **Presença de valores nulos**: Exclusão de registros com campos críticos vazios.
- **Detecção de outliers**: Identificação e tratamento de valores atípicos usando **Z-score**.
- **Distribuição dos dados**: Verificação de anomalias estatísticas.
- **Formato e integridade das datas**: Conversão de formatos e análise de consistência temporal.

## 🛠️ Como Rodar o Projeto
### 📌 Pré-requisitos
- Conta AWS com permissões para Glue e S3.
- Python 3.8+
- PySpark instalado (`pip install pyspark`)

### 📌 Configuração do Ambiente
1. **Clone o repositório:**
```sh
 git clone https://github.com/vivinfor/aws-glue-pyspark-etl.git
 cd aws-glue-pyspark-etl
```
2. **Crie um ambiente virtual e instale as dependências:**
```sh
 python -m venv venv
 source venv/bin/activate  # Linux/Mac
 venv\Scripts\activate  # Windows
 pip install -r requirements.txt
```
3. **Defina as variáveis de ambiente** no `.env`:
```sh
 AWS_ACCESS_KEY=your-access-key
 AWS_SECRET_KEY=your-secret-key
 S3_BUCKET_NAME=your-bucket
```
4. **Execute o script PySpark localmente:**
```sh
 python etl.py
```

## 📊 Resultados e Análises
- **Estatísticas descritivas antes e depois da transformação.**
- **Detecção e tratamento de outliers.**
- **Validação da qualidade dos dados pós-ETL.**

## 🔄 Próximos Passos
- Integrar com **AWS Lambda** para execução automatizada do ETL.
- Criar uma **estrutura de particionamento** para otimizar queries.
- Implementar **testes automatizados** de qualidade dos dados.
- Criar dashboards com **Plotly** ou **Power BI** para análise de fraudes.

🚀 **Desenvolvido por [Viviana]**
