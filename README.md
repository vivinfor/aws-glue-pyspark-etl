# Fraud Detection ETL Pipeline

Pipeline de ETL em PySpark para processamento de transações financeiras com identificação de padrões de fraude, expondo os dados processados via API REST.

Desenvolvido para execução local e deployment no **Google Cloud Platform (GCP)**, usando **Cloud Storage** para armazenamento e **Cloud Run** para servir a API.

## Contexto de uso

Este pipeline resolve a etapa de **transformação e disponibilização**, não de ingestão. O ponto de partida é um CSV já disponível, entregue por qualquer processo de ingestão existente na empresa (Airbyte, Fivetran, export de ERP, job próprio).

O que o pipeline agrega a partir daí:

- **Qualidade dos dados:** regras de validação, nulos e outliers configuráveis via YAML, sem necessidade de alterar código
- **Dado estruturado:** saída em Parquet particionado, pronto para consumo por ferramentas de analytics
- **Cálculo centralizado:** a API garante que métricas de fraude são calculadas uma única vez e de forma consistente para todos os consumidores

Casos de uso típicos: fintechs e bancos com export diário do core bancário, operadoras de cartão com dados de antifraude de terceiros, e-commerces que recebem relatórios de transações e precisam de uma camada de qualidade antes de cruzar com dados internos.

## Arquitetura

```
CSV (transações brutas)
  → ETL PySpark        (limpeza, enriquecimento, validação)
  → Parquet            (particionado por categoria)
  → FastAPI            (endpoints de consulta sobre os dados processados)
```

**Armazenamento no GCP:** os arquivos Parquet são salvos no **Google Cloud Storage (GCS)**. Localmente, ficam em `data/optimized/`.

## Tecnologias

| Camada | Local | GCP |
|--------|-------|-----|
| Processamento | PySpark (standalone) | Dataproc |
| Armazenamento | `data/optimized/` (Parquet) | Cloud Storage (GCS) |
| API | FastAPI + Uvicorn | Cloud Run |

## Estrutura do projeto

```
fraud-etl/
├── pipeline/                  # Módulos ETL (em desenvolvimento)
│   ├── utils.py               # Funções compartilhadas (config, Spark session, validações)
│   ├── extract.py             # Leitura do CSV
│   ├── transform.py           # Enriquecimento e validação
│   └── load.py                # Salvamento em Parquet (local ou GCS)
├── api/                       # FastAPI (em desenvolvimento)
│   ├── main.py
│   └── routes/
│       └── transactions.py
├── tests/                     # Testes unitários (em desenvolvimento)
│   ├── fixtures/
│   │   └── sample.csv         # Amostra do dataset para testes
│   └── test_transform.py
├── scripts/                   # Scripts originais (base para o refactor)
│   ├── etl_pipeline.py
│   ├── data_validation.py
│   └── save_optimized_data.py
├── config/
│   ├── schema.json            # Schema das colunas e tipos esperados
│   ├── validation_rules.yaml  # Regras de validação (nulos, outliers, fraudes)
│   └── config.yaml            # Caminhos e configurações gerais
├── run_pipeline.py            # Entry point único (em desenvolvimento)
├── Dockerfile
└── requirements.txt
```

## Estado atual

| Componente | Estado |
|-----------|--------|
| ETL (extract, transform, load) | Implementado nos `scripts/` |
| Configuração externalizada | Implementado |
| Refactor para `pipeline/` com utils compartilhado | Em desenvolvimento |
| API FastAPI | Em desenvolvimento |
| Testes unitários | Em desenvolvimento |

## O que o ETL faz

**Extração:** lê CSV de transações financeiras com separador `|`.

**Transformação:**
- combina colunas de data e hora em `trans_date_trans_time`
- cria `hour_of_day`, `day_of_week` e `transaction_period` (Madrugada / Manhã / Tarde / Noite)
- remove registros com nulos em colunas críticas (`cc_num`, `amt`, `is_fraud`)
- preenche nulos em colunas não críticas com valores padrão configuráveis
- remove outliers em `amt` por z-score com threshold configurável

**Carga:** salva em Parquet particionado por `category` (local ou GCS via `gs://`).

## API (endpoints planejados)

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/summary` | Total de transações, fraudes e média de valor |
| GET | `/fraud/by-category` | Fraudes agrupadas por categoria |
| GET | `/fraud/by-period` | Fraudes por período do dia |

## Como executar

### Pré-requisitos

```bash
pip install -r requirements.txt
```

### Executar o pipeline localmente

```bash
# Enquanto o refactor não está concluído, executar na sequência:
python scripts/etl_pipeline.py
python scripts/save_optimized_data.py
```

Os dados processados são salvos em `data/optimized/` particionados por `category`.

### Executar no GCP

Para apontar para o Cloud Storage, basta alterar `optimized_data_path` no `config/config.yaml`:

```yaml
environment: gcp
optimized_data_path: "gs://seu-bucket/optimized/"
```

A API pode ser deployada no Cloud Run com o `Dockerfile` existente.

## Dataset

O pipeline foi desenvolvido com um dataset de transações financeiras contendo sinalizações de fraude. Pode ser adaptado para outros datasets ajustando `config/schema.json` e `config/validation_rules.yaml`.

Campos principais: `cc_num`, `amt`, `merchant`, `category`, `is_fraud`, `trans_date_trans_time`, coordenadas geográficas e dados do titular do cartão.

---

Desenvolvido por [Viviana](https://github.com/vivinfor)
