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
    participant S3 as Armazém de Dados (S3)
    participant API as API Externa
    participant Glue as AWS Glue (PySpark)
    participant Storage as Banco de Dados (PostgreSQL)
    participant FastAPI as API de Consulta
    participant BI as Dashboard (Power BI)

    API->>Glue: Extração de Dados de API
    S3->>Glue: Extração de Dados do S3
    Glue->>Glue: Transformação e Limpeza
    Glue->>Storage: Armazenamento Processado
    FastAPI->>Storage: Consulta de Dados
    BI->>FastAPI: Visualização via API
