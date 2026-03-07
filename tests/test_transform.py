from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from pipeline.transform import enrich
from pipeline.utils import detect_outliers_zscore, validate_nulls


def test_transaction_period_mapping(spark: SparkSession):
    """
    Verifica se enrich() mapeia corretamente o horário da transação
    para o período do dia esperado.
    """
    data = [
        (datetime(2020, 1, 1, 3, 15),),   # Madrugada
        (datetime(2020, 1, 1, 9, 30),),   # Manhã
        (datetime(2020, 1, 1, 14, 45),),  # Tarde
        (datetime(2020, 1, 1, 20, 0),),   # Noite
    ]
    df = spark.createDataFrame(data, ["trans_date_trans_time"])
    df = enrich(df)

    periods = [row["transaction_period"] for row in df.orderBy("trans_date_trans_time").collect()]
    assert periods == ["Madrugada", "Manhã", "Tarde", "Noite"]


def test_null_drop_critical(spark: SparkSession):
    """
    Verifica se validate_nulls() remove registros com nulos em colunas
    críticas (cc_num).
    """
    data = [
        ("txn_001", "4532015112830001", 50.0),
        ("txn_002", None, 75.0),  # nulo crítico — deve ser removido
        ("txn_003", "4532015112830003", 30.0),
    ]
    df = spark.createDataFrame(data, ["trans_num", "cc_num", "amt"])
    df = validate_nulls(df, critical_cols=["cc_num"], fill_values={})

    remaining = [row["trans_num"] for row in df.collect()]
    assert "txn_002" not in remaining
    assert len(remaining) == 2


def test_null_fill_non_critical(spark: SparkSession):
    """
    Verifica se validate_nulls() preenche nulos em colunas não críticas
    com o valor padrão definido (merchant → "Desconhecido").
    """
    data = [
        ("txn_001", "Merchant A", 50.0),
        ("txn_002", None, 75.0),  # nulo não crítico — deve ser preenchido
    ]
    df = spark.createDataFrame(data, ["trans_num", "merchant", "amt"])
    df = validate_nulls(df, critical_cols=[], fill_values={"merchant": "Desconhecido"})

    merchants = {row["trans_num"]: row["merchant"] for row in df.collect()}
    assert merchants["txn_002"] == "Desconhecido"
    assert merchants["txn_001"] == "Merchant A"


def test_outlier_removal(spark: SparkSession):
    """
    Verifica se detect_outliers_zscore() remove registros cujo amt
    ultrapassa 3 desvios padrão da média.
    """
    # 20 registros normais + 1 outlier claro
    normal = [("txn_{:03d}".format(i), 75.0) for i in range(20)]
    outlier = [("txn_out", 8999.99)]
    df = spark.createDataFrame(normal + outlier, ["trans_num", "amt"])

    df = detect_outliers_zscore(df, col_name="amt", threshold=3)

    trans_nums = [row["trans_num"] for row in df.collect()]
    assert "txn_out" not in trans_nums
    assert len(trans_nums) == 20
