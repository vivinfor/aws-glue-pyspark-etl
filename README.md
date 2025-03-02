# AWS Glue & PySpark ETL

## 📌 Objetivo
Este projeto implementa um **pipeline de ETL escalável** usando **AWS Glue e PySpark** para processamento, transformação e validação de dados transacionais. O pipeline garante qualidade, consistência e eficiência no tratamento de grandes volumes de dados, aplicando técnicas avançadas para **limpeza, remoção de outliers, validação de schema e enriquecimento dos dados**.

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
   - Remoção de **valores nulos e duplicados** com base em regras de negócio.
   - **Validação da estrutura do dataset** de acordo com um schema predefinido (`schema.json`).
   - **Detecção e tratamento de outliers** usando **Z-score**.
   - **Criação de novas colunas** para facilitar análises (dia da semana, horário da transação, etc.).
   - Cálculo de **estatísticas descritivas** (média, mediana, desvio padrão, distribuição de valores faltantes).
   - Normalização de colunas para padronização dos dados.
3. **Carga**: Salvamento do dataset transformado no S3, pronto para análise posterior.

## 🔹 Regras de Negócio Aplicadas
Durante o pipeline ETL, foram aplicadas regras para garantir a **qualidade dos dados e detecção de possíveis fraudes**:

### **1️⃣ Tratamento de Valores Nulos**
| Coluna | Regra |
|--------|-------|
| `cc_num` (Cartão) | 🚨 **Obrigatório** – Remover registros sem número de cartão. |
| `amt` (Valor) | 🚨 **Obrigatório** – Transações sem valor devem ser descartadas. |
| `merchant` (Comerciante) | 🔹 **Opcional** – Se vazio, preencher com `"Desconhecido"`. |
| `city`, `state` | 🔹 **Opcional** – Se vazios, preencher com `"Não informado"`. |
| `lat`, `long` | 🔹 **Opcional** – Se vazios, preencher com `0.0`. |
| `is_fraud` (Fraude) | 🚨 **Obrigatório** – Se `null`, o registro deve ser descartado. |

### **2️⃣ Detecção e Remoção de Outliers**
| Coluna | Regra |
|--------|-------|
| `amt` (Valor) | 🚨 **Remover transações fora de 3 desvios padrão (Z-score).** |
| `city_pop` (População) | 🚨 **Remover registros com `city_pop < 100` (cidades não realistas).** |

### **3️⃣ Regras de Detecção de Fraudes**
| Critério | Regra |
|----------|-------|
| **Transações acima de $10,000** | 🚨 **Alerta de possível fraude** |
| **Mesma pessoa comprando em estados diferentes no mesmo dia** | 🚨 **Alerta de possível fraude** |
| **Múltiplas transações no mesmo comerciante em menos de 10 segundos** | 🚨 **Alerta de possível fraude** |

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
- **Identificação de padrões de fraudes baseados em regras de negócio.**
- **Validação da qualidade dos dados pós-ETL.**

## 🔄 Próximos Passos
- Integrar com **AWS Lambda** para execução automatizada do ETL.
- Criar uma **estrutura de particionamento** para otimizar queries.
- Implementar **testes automatizados** de qualidade dos dados.
- Criar dashboards com **Plotly** ou **Power BI** para análise de fraudes.

🚀 Desenvolvido por [Viviana](https://github.com/vivinfor)
