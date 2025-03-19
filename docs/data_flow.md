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
graph LR;
    
    %% Subgrupos para melhor organização
    subgraph Ingestão
        A1[APIs]
        A2[Bancos de Dados]
        A3[Arquivos (S3)]
    end

    subgraph Processamento
        B[AWS Glue (PySpark)]
    end

    subgraph Armazenamento
        C1[Dados Brutos (S3)]
        C2[Métricas Processadas (PostgreSQL)]
    end

    subgraph Exposição
        D[FastAPI]
        E[Dashboard (Power BI)]
    end

    %% Fluxo de Dados
    A1 -->|Extração| B
    A2 -->|Extração| B
    A3 -->|Extração| B

    B -->|Transformação| C1
    B -->|Carga| C2

    D -->|Consulta| C2
    E -->|Visualização| D
