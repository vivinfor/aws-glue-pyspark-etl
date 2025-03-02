# AWS Glue & PySpark ETL

## 📌 Objetivo
Este projeto implementa um **pipeline de ETL escalável** usando **AWS Glue e PySpark** para processamento, transformação e validação de dados transacionais. O pipeline garante qualidade, consistência e eficiência no tratamento de grandes volumes de dados, aplicando técnicas avançadas para **limpeza, remoção de outliers, validação de schema e enriquecimento dos dados**. Além disso, agora exploramos **otimização de consultas SQL**, **análises avançadas com Python** e **visualização interativa de dados com Power BI**.

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
- **Jupyter Notebooks**: Para exploração e visualização dos dados.
- **Power BI**: Para construção de dashboards interativos.

## 🚀 Pipeline de ETL e Análises
1. **Extração**: Carregamento do dataset público armazenado no **S3**.
2. **Transformação**:
   - Remoção de **valores nulos e duplicados** com base em regras de negócio.
   - **Validação da estrutura do dataset** de acordo com um schema predefinido (`config/schema.json`).
   - **Detecção e tratamento de outliers** usando **Z-score**.
   - **Criação de novas colunas** para facilitar análises (dia da semana, horário da transação, etc.).
   - Cálculo de **estatísticas descritivas** (média, mediana, desvio padrão, distribuição de valores faltantes).
   - Normalização de colunas para padronização dos dados.
3. **Otimização de Consultas SQL**:
   - Comparação entre **SQL puro, PySpark e Delta Lake**.
   - Testes de performance e eficiência nas consultas.
4. **Carga e Visualização**:
   - Salvamento do dataset transformado no S3 e banco de dados.
   - Integração com **Power BI** para construção de dashboards interativos.

## 🔹 Regras de Negócio Aplicadas
Durante o pipeline ETL, foram aplicadas regras para garantir a **qualidade dos dados e detecção de possíveis fraudes**:

### **1️⃣ Tratamento de Valores Nulos**
| Coluna | Regra |
|--------|-------|
| `cc_num` (Cartão) | 🚨 **Obrigatório** – Remover registros sem número de cartão. |
| `amt` (Valor) | 🚨 **Obrigatório** – Transações sem valor devem ser descartadas. |
| `merchant` (Comerciante) | 🔹 **Opcional** – Se vazio, preencher com "Desconhecido". |
| `city`, `state` | 🔹 **Opcional** – Se vazios, preencher com "Não informado". |
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

## 🔹 Estrutura do Projeto
```
aws-glue-pyspark-etl/
├── data/                          # Dados brutos e processados (não versionados)
├── notebooks/                     # Jupyter Notebooks com SQL + Python
│   ├── 01_data_exploration.ipynb  # Análise exploratória inicial
│   ├── 02_etl_processing.ipynb    # Pipeline de ETL no PySpark
│   ├── 03_sql_optimization.ipynb  # Comparação de SQL puro x PySpark
│   ├── 04_power_bi_integration.ipynb  # Transformação de dados para Power BI
├── scripts/                       # Código fonte em Python
├── sql/                           # Consultas SQL otimizadas
├── power_bi/                      # Dashboard Power BI
├── docs/                          # Documentação adicional
├── config/                        # Arquivos de configuração
│   ├── schema.json                # Especificação do schema para validação de dados
└── README.md                      # Documentação principal do projeto
```

## 📊 Resultados e Análises
- **Estatísticas descritivas antes e depois da transformação.**
- **Otimização de consultas SQL e comparação de performance.**
- **Dashboards interativos para análise de fraudes.**
- **Validação da qualidade dos dados pós-ETL.**

## 📔 Notebooks Criados
1. **01_data_exploration.ipynb** → Análise exploratória e estatísticas iniciais.
2. **02_etl_processing.ipynb** → Implementação do pipeline ETL no PySpark.
3. **03_sql_optimization.ipynb** → Comparação entre SQL puro e PySpark, otimizando queries.
4. **04_power_bi_integration.ipynb** → Preparação de dados para visualização no Power BI.

## 🔄 Próximos Passos
- Implementar **estrutura de particionamento** para otimizar queries.
- Criar dashboards com **Power BI** para visualização de padrões de fraude.
- Adicionar **testes automatizados** de qualidade dos dados.
- Integrar com **AWS Lambda** para execução automatizada do ETL.

🚀 Desenvolvido por [Viviana](https://github.com/vivinfor)

