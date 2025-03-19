# Arquitetura do AWS Glue PySpark ETL

## Visão Geral
Este projeto implementa um pipeline de ETL utilizando AWS Glue, PySpark e FastAPI para ingestão, transformação e exposição de dados para análises.

## Tecnologias Utilizadas
- **AWS Glue**: Gerenciamento de jobs PySpark escaláveis.
- **S3**: Armazenamento de dados brutos e transformados.
- **PySpark**: Processamento distribuído dos dados.
- **FastAPI**: Exposição dos dados processados através de uma API REST.
- **PostgreSQL**: Banco de dados para persistência de métricas agregadas.
- **Docker**: Para ambiente de desenvolvimento local.

## Diagrama da Arquitetura
```mermaid
graph TD;
    A[Dados Brutos (S3)] -->|Extração| B[ETL AWS Glue]
    B -->|Transformação| C[Armazenamento S3 - Parquet]
    B -->|Carga| D[PostgreSQL]
    D -->|Consulta| E[FastAPI]
    E -->|Exposição| F[Dashboard Power BI]
