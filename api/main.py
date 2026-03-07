import logging
import pandas as pd
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import transactions
from pipeline.utils import load_config

CONFIG_PATH = "config/config.yaml"

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Carrega os dados processados do Parquet na inicialização da API.
    Se os dados não existirem, a API sobe em modo degradado e retorna
    503 nos endpoints de dados até que o pipeline seja executado.
    """
    config = load_config(CONFIG_PATH)
    try:
        app.state.df = pd.read_parquet(config["optimized_data_path"])
        logger.info("Data loaded successfully from %s", config["optimized_data_path"])
    except FileNotFoundError:
        app.state.df = None
        logger.warning(
            "Data not found at '%s'. Run the pipeline first: python run_pipeline.py",
            config["optimized_data_path"],
        )
    yield


app = FastAPI(
    title="Fraud Detection API",
    description="API para consulta de métricas de fraude em transações financeiras processadas pelo ETL PySpark.",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(transactions.router)


@app.get("/", tags=["health"])
def health():
    """Verifica se a API está no ar."""
    return {"status": "ok"}
