# **AWS Glue, PySpark & FastAPI ETL**

## 📌 **Objetivo**
Este projeto implementa um **pipeline de ETL escalável** combinando **AWS Glue, PySpark e FastAPI** para processamento, transformação e disponibilização de dados transacionais.

Além das etapas tradicionais de ETL, este projeto se destaca por:  
✅ **Otimização de consultas SQL e PySpark** para melhor performance.  
✅ **Uso de uma abordagem híbrida** → **Banco de dados para métricas calculadas** e **Parquet para armazenar dados brutos e históricos**.  
✅ **Exposição de dados via FastAPI**, centralizando cálculos no backend para evitar inconsistências.  
✅ **Criação de um dataset otimizado para Power BI** e análise de fraudes financeiras.  
✅ **Execução do ETL validada tanto no VS Code (Spark Standalone) quanto no Jupyter.**  

---

## 👉 **Origem e Estrutura dos Dados**

Este projeto foi desenvolvido para processar e otimizar **transações financeiras fictícias**, aplicando regras de validação e detecção de fraudes para garantir a qualidade dos dados antes da análise.

### 📚 **Estrutura dos Dados**

Os dados são estruturados para incluir:

✅ **Detalhes da transação** → Data, valor (`amt`), comerciante (`merchant`), categoria (`category`).  
✅ **Informações do cliente** → Localização, profissão, identificador único.  
✅ **Sinalização de fraudes** → Identificação de padrões suspeitos para análise posterior.  

### 👀 **Foco do Projeto**

Este projeto não se trata apenas de carregar um conjunto de dados, mas sim de **demonstrar boas práticas de ETL, otimização de dados e criação de insights estruturados para análise de fraudes**.

💡 **Importante:** O pipeline foi projetado para garantir **coerência e integridade** nas informações processadas, aplicando validações antes da carga e garantindo que os dados analisados sejam consistentes com as regras de negócio definidas.

---

## 🔹 **Tecnologias Utilizadas**
💾 **Processamento e Transformação**  
✅ **AWS Glue & PySpark** → Para manipulação de grandes volumes de dados.  
✅ **SQL (AWS Athena)** → Para consultas eficientes e validação de dados.  

📊 **Análise e Visualização**  
✅ **Jupyter Notebook** → Para análise exploratória e experimentação com consultas otimizadas.  
✅ **Power BI** → Para construção de **dashboards interativos** sobre fraudes.  
✅ **FastAPI** → Exposição dos cálculos como API para garantir consistência.  

🚀 **Armazenamento e Integração**  
✅ **AWS S3 + Parquet** → Para armazenamento eficiente dos dados brutos e históricos.  
✅ **PostgreSQL** → Para armazenamento de métricas pré-calculadas e acesso rápido na API.  
✅ **Delta Lake** → Suporte a atualizações incrementais.  

---

## 🚀 **Arquitetura Híbrida (Parquet + Banco de Dados + API)**

Este projeto adota uma **abordagem híbrida**, combinando **Parquet, banco de dados e API** para maximizar desempenho e escalabilidade:

1️⃣ **Parquet (AWS S3 / Local)** → Armazena **dados brutos e históricos** otimizados para leitura no Power BI.  
2️⃣ **PostgreSQL** → Centraliza **métricas calculadas**, garantindo performance para análises frequentes.  
3️⃣ **FastAPI** → Expõe **dados pré-processados** para evitar cálculos repetitivos no Power BI.  

### **📌 Vantagens da Arquitetura**
✅ **Centralização dos cálculos no Python** evita inconsistências entre usuários.  
✅ **Banco de Dados para métricas** → Respostas rápidas via API, reduzindo processamento no Power BI.  
✅ **Parquet para histórico** → Mantém dados brutos disponíveis para consultas avançadas.  
✅ **API padronizada** → Integrações consistentes entre aplicações e análises de negócio.  

---

## 🚀 **Endpoints da API FastAPI**
A API expõe métricas **pré-calculadas**, reduzindo a carga computacional no Power BI e padronizando cálculos.  

| Método | Endpoint | Descrição |
|--------|---------|------------|
| **GET** | `/transactions/summary` | Retorna total de transações, fraudes e média de valores. |
| **GET** | `/fraud/monthly` | Retorna o total de fraudes por mês. |
| **GET** | `/fraud/category` | Retorna fraudes agregadas por categoria de transação. |
| **GET** | `/fraud/high_value` | Lista transações suspeitas acima de $10.000. |
| **GET** | `/anomalies/outliers` | Identifica transações com valores fora do padrão. |

**Exemplo de Uso:**
```sh
curl -X GET "http://localhost:8000/transactions/summary"
```

---

## 📊 **Formato do Arquivo Final para Power BI**
| Formato | Motivo |
|---------|--------|
| **Parquet** | 🚀 Compactação melhor e leitura rápida no Power BI. |
| **PostgreSQL** | 🔄 Permite armazenamento de métricas calculadas para acesso via API. |
| **CSV** | ❌ Não utilizado, pois ocupa mais espaço e tem leitura lenta. |

📌 **Decisão final:**  
- **API FastAPI + PostgreSQL** para métricas acessadas com frequência.  
- **Parquet para armazenamento de dados históricos e carregamento no Power BI.**  

---

## 👉 Estrutura do Projeto

```md
aws-glue-pyspark-etl/
├── data/                  # Dados brutos e processados
├── notebooks/             # Jupyter Notebook para análise exploratória
│   ├── 01_data_exploration.ipynb  # Análise exploratória, otimização e insights
├── scripts/               # Código ETL em Python (VS Code)
├── api/                   # Implementação da API FastAPI
├── power_bi/              # Dashboard Power BI
├── config/                
│   ├── schema.json        # Definição do schema dos dados
│   ├── settings.yaml      # Configurações gerais
└── README.md              # Documentação do projeto
```

---

## 🔄 **Próximos Passos**
✅ **Implementação da API**  para expor métricas calculadas, garantindo acesso rápido e estruturado aos dados.
✅ **Monitoramento da API** para acompanhar tempo de resposta e acessos.  
✅ **Criação de uma camada de cache (Redis) para reduzir consultas repetitivas.**  
✅ **Aprimorar detecção de anomalias com aprendizado de máquina.**  
✅ **Testar escalabilidade com maior volume de dados.**  
✅ **Criar documentação completa para consumo da API.**  

---

📌 **Desenvolvido por** [Viviana](https://github.com/vivinfor) 🚀

