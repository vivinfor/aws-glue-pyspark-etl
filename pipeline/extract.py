import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, lit, to_timestamp

logger = logging.getLogger(__name__)


def load_csv(spark: SparkSession, path: str) -> DataFrame:
    """
    Lê o CSV com separador pipe e combina as colunas trans_date e trans_time
    em trans_date_trans_time, removendo as originais.
    """
    df = spark.read.option("sep", "|").option("header", True).csv(path)
    logger.info(f"Loaded CSV from {path}")

    if "trans_date" in df.columns and "trans_time" in df.columns:
        df = df.withColumn(
            "trans_date_trans_time",
            to_timestamp(
                concat(col("trans_date"), lit(" "), col("trans_time")),
                "yyyy-MM-dd HH:mm:ss",
            ),
        ).drop("trans_date", "trans_time")

    return df
