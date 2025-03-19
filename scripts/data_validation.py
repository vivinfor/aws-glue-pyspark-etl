import os
import json
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, mean, stddev, unix_timestamp, lag, date_format, lit
)
from pyspark.sql.window import Window

# 📌 Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 📂 Configuração dos caminhos
CONFIG_PATH = "config/config.yaml"
SCHEMA_PATH = "config/schema.json"
VALIDATION_RULES_PATH = "config/validation_rules.yaml"

# 📂 Verificar se os arquivos de configuração existem
for path in [CONFIG_PATH, SCHEMA_PATH, VALIDATION_RULES_PATH]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Arquivo '{path}' não encontrado!")

# 📂 Carregar os arquivos de configuração
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

with open(SCHEMA_PATH, "r") as f:
    schema = json.load(f)

with open(VALIDATION_RULES_PATH, "r") as f:
    validation_rules = yaml.safe_load(f)

# 📂 Determinar caminho de dados processados
DATA_PATH = os.path.abspath(config["data_path"])
logger.info(f"📂 Diretório de dados processados: {DATA_PATH}")

# 🚀 Criar sessão Spark
spark = SparkSession.builder.appName("Validação de Dados - ETL").getOrCreate()

# 📂 Carregar os dados processados
df = spark.read.parquet(DATA_PATH)

# ✅ **1. Verificar colunas esperadas**
expected_columns = {field["name"] for field in schema["fields"]}
actual_columns = set(df.columns)
missing_columns = expected_columns - actual_columns

if missing_columns:
    logger.warning(f"⚠️ Colunas ausentes no dataset: {missing_columns}")
else:
    logger.info("✅ Todas as colunas esperadas estão presentes.")

# ✅ **2. Validar tipos de dados**
type_mapping = {
    "string": "string",
    "int": "integer",
    "double": "double",
    "timestamp": "timestamp"
}

type_issues = []
for field in schema["fields"]:
    col_name = field["name"]
    expected_type = type_mapping.get(field["type"], "unknown")
    
    if col_name in df.columns:
        actual_type = df.select(col_name).schema[0].dataType.simpleString()
        if expected_type == "double":
            expected_type = "float"
        if actual_type != expected_type:
            type_issues.append(f"⚠️ Tipo incorreto em `{col_name}`: Esperado {expected_type}, encontrado {actual_type}")

if type_issues:
    for issue in type_issues:
        logger.warning(issue)
else:
    logger.info("✅ Todos os tipos de dados estão corretos.")

# ✅ **3. Verificar valores nulos críticos**
for col_name in validation_rules["validation"]["missing_values"]["critical"]:
    null_count = df.filter(col(col_name).isNull()).count()
    if null_count > 0:
        logger.error(f"❌ {null_count} registros possuem `{col_name}` nulo e devem ser removidos!")

df = df.dropna(subset=validation_rules["validation"]["missing_values"]["critical"])

# ✅ **4. Tratar valores nulos não críticos**
fill_values = validation_rules["validation"]["missing_values"]["non_critical"]
df = df.fillna(fill_values)
logger.info("✅ Valores nulos não críticos preenchidos conforme regras definidas.")

# ✅ **5. Detectar outliers em `amt`**
if validation_rules["validation"]["outlier_detection"]["amt"]["method"] == "zscore":
    threshold = validation_rules["validation"]["outlier_detection"]["amt"]["threshold"]
    amt_stats = df.select(mean("amt").alias("mean_amt"), stddev("amt").alias("std_amt")).collect()[0]
    mean_amt, std_amt = amt_stats["mean_amt"], amt_stats["std_amt"]
    outliers = df.filter((col("amt") > mean_amt + threshold * std_amt) | (col("amt") < mean_amt - threshold * std_amt))
    if outliers.count() > 0:
        logger.warning(f"⚠️ {outliers.count()} registros detectados como outliers em `amt`. Sugerido remover!")

# ✅ **6. Validação de fraudes**
if validation_rules["validation"]["fraud_detection"]["multi_state_check"]:
    df = df.withColumn("date", col("trans_date_trans_time").cast("date"))
    multi_state_purchases = df.groupBy("cc_num", "date").agg(count("state").alias("state_count")).filter(col("state_count") > 1)
    if multi_state_purchases.count() > 0:
        logger.warning(f"⚠️ {multi_state_purchases.count()} cartões usados em múltiplos estados no mesmo dia!")

# ✅ **7. Verificar duplicatas**
if validation_rules["validation"]["duplicates"]["check"]:
    duplicate_count = df.count() - df.dropDuplicates().count()
    if duplicate_count > 0:
        logger.warning(f"⚠️ {duplicate_count} registros duplicados encontrados!")
    else:
        logger.info("✅ Nenhuma duplicata encontrada.")

logger.info("✅ Validação de dados concluída com sucesso!")

# 🚀 **Encerrar sessão Spark**
spark.stop()
