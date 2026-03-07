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
├── pipeline/                  # Módulos ETL
│   ├── utils.py               # Funções compartilhadas (config, Spark session, validações)
│   ├── extract.py             # Leitura do CSV
│   ├── transform.py           # Enriquecimento e validação
│   └── load.py                # Salvamento em Parquet (local ou GCS)
├── api/                       # FastAPI
│   ├── main.py                # Configuração da app e carregamento dos dados
│   └── routes/
│       └── transactions.py    # Endpoints de fraude
├── tests/                     # Testes unitários
│   ├── conftest.py            # Fixture de SparkSession compartilhada
│   └── test_transform.py      # Testes de transformação e validação
├── config/
│   ├── schema.json            # Schema das colunas e tipos esperados
│   ├── validation_rules.yaml  # Regras de validação (nulos, outliers, fraudes)
│   └── config.yaml            # Caminhos e configurações gerais
├── data/
│   ├── raw/                   # CSV de entrada (não versionado)
│   └── optimized/             # Parquet gerado pelo pipeline (não versionado)
├── env/dev/env.yaml           # Variáveis de ambiente para desenvolvimento
├── run_pipeline.py            # Entry point único do ETL
├── Dockerfile                 # Imagem de produção
├── Dockerfile.dev             # Imagem de desenvolvimento (com hot-reload)
├── docker-compose.yml         # Configuração base
├── docker-compose.override.yml # Override automático para dev
├── Makefile                   # Comandos simplificados
└── requirements.txt
```

## O que o ETL faz

**Extração:** lê CSV de transações financeiras com separador `|`.

**Transformação:**
- combina colunas de data e hora em `trans_date_trans_time`
- cria `hour_of_day`, `day_of_week` e `transaction_period` (Madrugada / Manhã / Tarde / Noite)
- remove registros com nulos em colunas críticas (`cc_num`, `amt`, `is_fraud`)
- preenche nulos em colunas não críticas com valores padrão configuráveis
- remove outliers em `amt` por z-score com threshold configurável

**Carga:** salva em Parquet particionado por `category` em `data/optimized/` (local) ou `gs://` (GCP).

## API

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/summary` | Total de transações, fraudes, taxa de fraude e médias de valor |
| GET | `/fraud/by-category` | Contagem e taxa de fraude agrupadas por categoria |
| GET | `/fraud/by-period` | Contagem e taxa de fraude agrupadas por período do dia |

A API carrega os dados processados do Parquet na inicialização. Se o pipeline ainda não foi executado, ela sobe normalmente mas retorna `503` nos endpoints de dados.

## Como executar localmente

### Pré-requisitos

- Python 3.11+
- Java 8 ou 11 (necessário para o PySpark)
- Docker e Docker Compose (opcional, recomendado)

### Sem Docker

```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Colocar o CSV de entrada em data/raw/

# 3. Executar o pipeline para gerar os dados processados
python run_pipeline.py

# 4. Subir a API
uvicorn api.main:app --reload
```

A API ficará disponível em `http://localhost:8000` (porta padrão do Uvicorn).

### Com Docker (recomendado)

```bash
# Subir a API (com hot-reload)
make up-build

# Em outro terminal: executar o pipeline dentro do container
make pipeline

# Ver logs
make logs

# Parar
make down
```

A API ficará disponível em `http://localhost:8080`. Documentação interativa em `http://localhost:8080/docs`.

### Comandos disponíveis no Makefile

| Comando | Descrição |
|---------|-----------|
| `make up` | Inicia os containers |
| `make up-build` | Inicia e reconstrói os containers |
| `make pipeline` | Executa o ETL dentro do container |
| `make test` | Executa os testes dentro do container |
| `make logs` | Mostra logs em tempo real |
| `make shell` | Acessa o shell do container |
| `make down` | Para os containers |
| `make clean` | Remove containers e volumes |
| `make kill-port` | Mata processo rodando na porta 8080 |

### Executar testes

```bash
# Localmente
pytest tests/

# Dentro do container
make test
```

Os testes cobrem a camada de transformação, onde está a lógica de negócio principal:

| Teste | O que valida |
|-------|-------------|
| `test_transaction_period_mapping` | `enrich()` classifica corretamente o horário em Madrugada, Manhã, Tarde e Noite |
| `test_null_drop_critical` | registros com nulo em colunas críticas (`cc_num`) são removidos |
| `test_null_fill_non_critical` | nulos em colunas não críticas são preenchidos com o valor padrão configurado |
| `test_outlier_removal` | o filtro z-score remove transações com `amt` acima de 3 desvios padrão da média |

## Configuração

O arquivo `config/config.yaml` controla os caminhos e comportamento do pipeline:

```yaml
environment: local  # ou "gcp"

raw_data_path: "data/raw/"
optimized_data_path: "data/optimized"  # no GCP: "gs://seu-bucket/optimized/"

partition_keys: ["category"]
use_z_score_filter: true
compression: "snappy"
```

As regras de validação (nulos obrigatórios, valores padrão para preenchimento, threshold de outliers) ficam em `config/validation_rules.yaml`.

## Executar no GCP

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
