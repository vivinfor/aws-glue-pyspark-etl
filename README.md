# AWS Glue & PySpark ETL

## 📌 Objetivo
Este projeto demonstra um pipeline de **ETL escalável** usando **AWS Glue e PySpark** para processamento e transformação de dados. Ele inclui técnicas para **limpeza, tratamento de outliers, estatísticas descritivas e armazenamento otimizado no S3**.

## 🔹 Tecnologias Utilizadas
- **AWS Glue**: Para processamento e transformação de dados em grande escala.
- **PySpark**: Para manipulação e transformação eficiente dos dados.
- **AWS S3**: Para armazenamento dos dados brutos e processados.
- **SQL**: Para análise e validação de qualidade dos dados.

## 🚀 Pipeline de ETL
1. **Extração**: Carregamento de um dataset público (ex: transações financeiras, logs, etc.) armazenado no **S3**.
2. **Transformação**:
   - Remoção de **valores nulos e duplicados**.
   - **Detecção de outliers** usando **Z-score** e **IQR**.
   - Cálculo de **estatísticas descritivas** (média, mediana, desvio padrão, distribuição de valores faltantes).
   - Normalização de colunas.
3. **Carga**: Salvamento do dataset transformado no S3, pronto para análise posterior.

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

🚀 **Desenvolvido por [Viviana]**

