import logging

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, mean, stddev

logger = logging.getLogger(__name__)


def load_config(path: str) -> dict:
    """Carrega um arquivo YAML e retorna o conteúdo como dicionário."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def create_spark_session(app_name: str) -> SparkSession:
    """Cria e retorna uma SparkSession com o nome fornecido."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def validate_nulls(df: DataFrame, critical_cols: list, fill_values: dict) -> DataFrame:
    """
    Remove registros com nulos em colunas críticas e preenche nulos
    em colunas não críticas com os valores padrão fornecidos.
    """
    before = df.count()
    df = df.dropna(subset=critical_cols)
    dropped = before - df.count()

    if dropped > 0:
        logger.warning(f"Dropped {dropped} records with nulls in critical columns: {critical_cols}")

    df = df.fillna(fill_values)
    return df


def detect_outliers_zscore(df: DataFrame, col_name: str, threshold: float) -> DataFrame:
    """
    Remove registros cujo valor em col_name ultrapassa threshold desvios
    padrão da média (z-score). Retorna o DataFrame filtrado.
    """
    stats = df.select(
        mean(col_name).alias("mean"),
        stddev(col_name).alias("stddev"),
    ).collect()[0]

    col_mean, col_stddev = stats["mean"], stats["stddev"]

    df = df.filter(
        (col(col_name) <= col_mean + threshold * col_stddev)
        & (col(col_name) >= col_mean - threshold * col_stddev)
    )

    logger.info(f"Outlier filter applied: {col_name} within {threshold} std devs of mean ({col_mean:.2f})")
    return df
