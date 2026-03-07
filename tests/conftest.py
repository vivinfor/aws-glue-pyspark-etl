import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession local compartilhada entre todos os testes da sessão."""
    session = (
        SparkSession.builder
        .appName("fraud-etl-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
