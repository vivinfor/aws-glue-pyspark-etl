import pandas as pd
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import transactions
from pipeline.utils import load_config

CONFIG_PATH = "config/config.yaml"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Carrega os dados processados do Parquet na inicialização da API."""
    config = load_config(CONFIG_PATH)
    app.state.df = pd.read_parquet(config["optimized_data_path"])
    yield


app = FastAPI(title="Fraud Detection API", lifespan=lifespan)
app.include_router(transactions.router)
