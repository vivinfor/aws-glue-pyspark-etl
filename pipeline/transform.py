import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, lit, when
from pyspark.sql.types import DoubleType, IntegerType

from pipeline.utils import detect_outliers_zscore, validate_nulls

logger = logging.getLogger(__name__)

# Mapeamento de colunas para seus tipos corretos
_TYPE_CASTS = {
    "zip": IntegerType(),
    "lat": DoubleType(),
    "long": DoubleType(),
    "city_pop": IntegerType(),
    "unix_time": IntegerType(),
    "amt": DoubleType(),
    "is_fraud": IntegerType(),
    "merch_lat": DoubleType(),
    "merch_long": DoubleType(),
}


def cast_types(df: DataFrame) -> DataFrame:
    """
    Converte colunas numéricas para seus tipos corretos.
    Necessário após leitura do CSV, que infere tudo como string.
    """
    for col_name, col_type in _TYPE_CASTS.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
    return df


def enrich(df: DataFrame) -> DataFrame:
    """
    Adiciona colunas derivadas da data/hora da transação:
    - hour_of_day: hora como inteiro (0-23)
    - day_of_week: dia da semana abreviado (Mon, Tue, ...)
    - transaction_period: Madrugada / Manhã / Tarde / Noite
    """
    df = df.withColumn(
        "hour_of_day",
        date_format(col("trans_date_trans_time"), "HH").cast("int"),
    )
    df = df.withColumn(
        "day_of_week",
        when(
            col("trans_date_trans_time").isNotNull(),
            date_format(col("trans_date_trans_time"), "E"),
        ).otherwise(lit("Unknown")),
    )
    df = df.withColumn(
        "transaction_period",
        when((col("hour_of_day") >= 0) & (col("hour_of_day") < 6), "Madrugada")
        .when((col("hour_of_day") >= 6) & (col("hour_of_day") < 12), "Manhã")
        .when((col("hour_of_day") >= 12) & (col("hour_of_day") < 18), "Tarde")
        .otherwise("Noite"),
    )
    return df


def validate(df: DataFrame, validation_rules: dict) -> DataFrame:
    """
    Aplica as regras de validação definidas em validation_rules.yaml:
    - remove nulos em colunas críticas
    - preenche nulos em colunas não críticas
    - remove outliers em amt por z-score
    """
    rules = validation_rules["validation"]

    df = validate_nulls(
        df,
        critical_cols=rules["missing_values"]["critical"],
        fill_values=rules["missing_values"]["non_critical"],
    )

    outlier_cfg = rules["outlier_detection"]["amt"]
    if outlier_cfg["method"] == "zscore":
        df = detect_outliers_zscore(df, "amt", outlier_cfg["threshold"])

    return df
