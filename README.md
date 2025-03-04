# **AWS Glue & PySpark ETL**

## 📌 **Objetivo**
Este projeto implementa um **pipeline de ETL escalável** usando **AWS Glue e PySpark** para processamento, transformação e validação de dados transacionais.  

Além das etapas tradicionais de ETL, este projeto se destaca por:  
✅ **Otimização de consultas SQL e PySpark** para melhor performance.  
✅ **Comparação entre diferentes abordagens** (SQL puro vs. PySpark vs. Delta Lake).  
✅ **Criação de um dataset otimizado para Power BI** e análise de fraudes financeiras.  
✅ **Execução do ETL validada tanto no VS Code (Spark Standalone) quanto no Jupyter.**  

---

## 🔹 **Fonte dos Dados**
Os dados vêm de um dataset público do Kaggle, gerado pelo **Sparkov Data Generation**, simulando transações financeiras de **janeiro a dezembro de 2023**.

### 📌 **Dataset**:
[Kaggle - Fraude em Transações de Cartão de Crédito](https://www.kaggle.com/competitions/fraude-em-transaes-de-carto-de-crdito/data)

**Principais colunas:**
- **Dados da transação:** Data, valor (`amt`), comerciante (`merchant`), categoria (`category`).
- **Dados do cliente:** Nome, localização, gênero, profissão.
- **Sinalizador de fraude (`is_fraud`).**

---

## 🔹 **Tecnologias Utilizadas**
💾 **Processamento e Transformação**  
✅ **AWS Glue & PySpark** → Para manipulação de grandes volumes de dados.  
✅ **SQL (AWS Athena)** → Para consultas eficientes e validação de dados.  

📊 **Análise e Visualização**  
✅ **Jupyter Notebook** → Para análise exploratória e experimentação com consultas otimizadas.  
✅ **Power BI** → Para construção de **dashboards interativos** sobre fraudes.  

🚀 **Armazenamento e Integração**  
✅ **AWS S3** → Para armazenamento eficiente dos dados.  
✅ **Parquet & Delta Lake** → Formatos otimizados para leitura rápida no Power BI.  

---

## 🚀 **Pipeline de ETL e Otimizações**
🔹 **1️⃣ Extração** → Carregamento do dataset bruto (CSV).  
🔹 **2️⃣ Transformação (ETL no PySpark)**
   - **Remoção de nulos e duplicatas.**
   - **Detecção e remoção de outliers com Z-score.**
   - **Conversão do schema para garantir consistência (ex: FLOAT → DOUBLE).**
   - **Criação de novas colunas úteis (hora, período do dia, tempo entre transações).**
   - **Comparação entre SQL puro, PySpark e Delta Lake.**
🔹 **3️⃣ Salvamento e Otimização**
   - **Formato Parquet** → Arquivo leve e eficiente para análise.
   - **Delta Lake** → Para suporte a updates incrementais.
   - **Comparação entre modos de escrita e impacto na performance.**
🔹 **4️⃣ Análise e Relatórios**
   - **Dashboards no Power BI** conectados diretamente ao dataset otimizado.

---

## 📊 **Otimização de Consultas SQL e PySpark**
- **🔥 Teste de performance:** Tempo de execução com SQL puro vs. PySpark.
- **📈 Estratégias aplicadas para reduzir tempo de leitura:**
  ✅ **Conversão de FLOAT para DOUBLE** antes do salvamento em Parquet.  
  ✅ **Uso de particionamento inteligente** (`partitionBy("category")`).  
  ✅ **Reparticionamento do DataFrame para paralelismo eficiente** (`df.repartition(4)`).  
  ✅ **Comparação entre Parquet e Delta Lake** em termos de eficiência.  

---

## 🔹 **Formato do Arquivo Final para Power BI**
| Formato | Motivo |
|---------|--------|
| **Parquet** | 🚀 Compactação melhor e leitura rápida no Power BI. |
| **Delta Lake** | 🔄 Permite atualizações incrementais nos dados. |
| **CSV** | ❌ Não utilizado, pois ocupa mais espaço e tem leitura lenta. |

📌 **Decisão final:**  
- O **Parquet foi escolhido** para o dataset final devido à **eficiência de leitura** no Power BI.  
- Caso seja necessário **atualizações incrementais**, a versão **Delta Lake** pode ser ativada.

---

## 🔹 **Estrutura do Projeto**

aws-glue-pyspark-etl/
├── data/                          # Dados brutos e processados
├── notebooks/                      # Jupyter Notebook para análise exploratória
│   ├── 01_data_exploration.ipynb   # Análise exploratória, otimização e insights
├── scripts/                        # Código ETL em Python (VS Code)
├── power_bi/                        # Dashboard Power BI
├── config/                         
│   ├── schema.json                 # Definição do schema dos dados
│   ├── settings.yaml                # Configurações gerais
└── README.md                        # Documentação do projeto

---

## 📊 **Resultados e Insights**
✅ **Melhoria de performance nas consultas** após conversão e otimização.  
✅ **Arquivos otimizados em Parquet**, reduzindo tempo de carregamento no Power BI.  
✅ **Dashboards interativos** analisando padrões de fraudes em transações.  

🚀 **Técnicas aplicadas para acelerar consultas**  
- **Particionamento correto dos dados** (`partitionBy("category")`).  
- **Uso de formatos eficientes** para integração com Power BI.  
- **Comparação de estratégias de escrita e impacto na performance.**  

---

## 📔 **Notebooks Criados**
1. **01_data_exploration.ipynb** → **Exploração, validação e otimização do dataset**.  

📌 **Diferente de projetos convencionais**, **toda a etapa de ETL foi validada no VS Code** para garantir um fluxo de dados escalável e eficiente, enquanto **o notebook foca na análise e otimização de consultas para exploração dos dados**.

---

## 🔄 **Próximos Passos**
✅ **Testar Delta Lake para atualização incremental.**  
✅ **Criar um relatório mais avançado no Power BI.**  
✅ **Adicionar logs e monitoramento da performance do ETL.**  
✅ **Testar diferentes tamanhos de partições e impacto no carregamento do Power BI.**  

---

📌 **Desenvolvido por** [Viviana](https://github.com/vivinfor)  
