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
sequenceDiagram
    participant Fonte as Fonte de Dados (S3, API)
    participant Glue as AWS Glue (PySpark)
    participant Storage as Armazenamento (S3, PostgreSQL)
    participant API as FastAPI
    participant BI as Dashboard (Power BI)

    Fonte->>Glue: Extração de Dados
    Glue->>Storage: Transformação e Armazenamento
    API->>Storage: Consultas
    BI->>API: Visualização de Dados