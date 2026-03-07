import logging
import os

from pipeline.extract import load_csv
from pipeline.load import save_parquet
from pipeline.transform import cast_types, enrich, validate
from pipeline.utils import create_spark_session, load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CONFIG_PATH = "config/config.yaml"
VALIDATION_RULES_PATH = "config/validation_rules.yaml"


def run() -> None:
    """Orquestra o pipeline completo: extract → transform → validate → load."""
    config = load_config(CONFIG_PATH)
    validation_rules = load_config(VALIDATION_RULES_PATH)

    spark = create_spark_session("Fraud Detection ETL")

    # --- Extract ---
    input_dir = os.path.abspath(config["raw_data_path"])
    csv_files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {input_dir}")

    df = load_csv(spark, os.path.join(input_dir, csv_files[0]))

    # --- Transform ---
    df = cast_types(df)
    df = enrich(df)
    df = validate(df, validation_rules)

    # --- Load ---
    output_path = os.path.abspath(config["optimized_data_path"])
    partition_keys = config.get("partition_keys", ["category"])
    save_parquet(df, output_path, partition_keys)

    spark.stop()
    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    run()
