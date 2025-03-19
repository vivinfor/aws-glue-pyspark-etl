# Fluxo de Dados

📌 **Objetivo:** Descrever o fluxo de dados do pipeline ETL, desde a ingestão até a exposição.

📝 **Dica:** Para visualizar melhor os diagramas desta documentação, utilize um editor que suporte [Mermaid.js](https://mermaid.js.org/), como **VSCode** com a extensão *Markdown Preview Enhanced*.

---

## 1. Extração
Os dados são extraídos de fontes externas, como:
- **APIs**
- **Bancos de dados**
- **Arquivos CSV/JSON armazenados no S3**

## 2. Transformação
O AWS Glue executa as seguintes transformações:
- **Limpeza de dados:** Remoção de valores nulos e duplicatas.
- **Conversão de tipos:** Garantia de consistência entre os tipos de dados.
- **Agregações:** Cálculo de métricas para análises.

## 3. Armazenamento
Os dados transformados são armazenados em dois formatos:
- **Parquet no S3:** Para análises futuras em ferramentas como Power BI.
- **PostgreSQL:** Para armazenamento de métricas e consultas rápidas.

## 4. Exposição
Os dados processados são expostos através da **API FastAPI**, permitindo acesso via endpoints REST.

---

## Diagrama do Fluxo de Dados
```mermaid
---
config:
  layout: fixed
---
flowchart TD
    A1["APIs Externas"] -- Extração de Dados --> B["AWS Glue PySpark"]
    A2["Bancos de Dados"] -- Extração de Dados --> B
    A3["Arquivos CSV JSON S3"] -- Extração de Dados --> B
    A4["Kafka"] -- Stream de Dados --> B
    B -- Validação de Dados --> V1["Regras de Validação e Qualidade"]
    V1 -- Remoção de Duplicatas --> B
    V1 -- Detecção de Fraudes --> B
    B -- Limpeza e Padronização --> C["Dados Transformados (Parquet)"]
    B -- Cálculo de Métricas --> D["Dados Agregados"]
    B -- Identificação de Outliers --> O["Análise de Outliers"]
    C -- Armazenamento --> E["S3 Data Lake"]
    D -- Carga Final --> F["PostgreSQL Data Warehouse"]
    O -- Armazenamento --> G["Delta Lake para Histórico"]
    F -- Consulta de Dados --> G1["API de Consulta (FastAPI)"]
    G1 -- Exibição --> H["Dashboard Power BI"]
    G1 -- Integração --> H2["Outras Ferramentas (Tableau, Looker)"]
    G1 -- Registro de Logs --> M["Monitoramento e Auditoria"]
    G1 -- Controle de Acesso --> S["Autenticação e Segurança (JWT, OAuth)"]
