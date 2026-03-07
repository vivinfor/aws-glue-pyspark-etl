import logging

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

# Colunas internas que não devem ir para o dado final
_COLUMNS_TO_DROP = {"profile", "acct_num", "time_diff"}

# Valor padrão para category evita a partição __HIVE_DEFAULT_PARTITION__ no Parquet
_CATEGORY_DEFAULT = "desconhecido"


def save_parquet(df: DataFrame, path: str, partition_by: list) -> None:
    """
    Remove colunas internas, garante valor padrão em category e salva
    o DataFrame em Parquet particionado. Aceita path local ou gs://.
    """
    cols_to_drop = _COLUMNS_TO_DROP & set(df.columns)
    if cols_to_drop:
        df = df.drop(*cols_to_drop)

    df = df.fillna({"category": _CATEGORY_DEFAULT})

    df.write.mode("overwrite").partitionBy(*partition_by).parquet(path)
    logger.info(f"Data saved to {path} partitioned by {partition_by}")
