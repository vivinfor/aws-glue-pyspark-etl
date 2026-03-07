from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter()


def _get_df(request: Request):
    """Retorna o DataFrame carregado ou levanta 503 se o pipeline ainda não foi executado."""
    df = request.app.state.df
    if df is None:
        raise HTTPException(
            status_code=503,
            detail="Data not available. Run the pipeline first: python run_pipeline.py",
        )
    return df


class TransactionSummary(BaseModel):
    total_transactions: int
    total_fraud: int
    fraud_rate_pct: float
    avg_transaction_value: float
    avg_fraud_value: float


class FraudByCategory(BaseModel):
    category: str
    total_fraud: int
    fraud_rate_pct: float


class FraudByPeriod(BaseModel):
    period: str
    total_fraud: int
    fraud_rate_pct: float


@router.get("/summary", response_model=TransactionSummary)
def get_summary(request: Request):
    """Retorna totais gerais de transações e métricas de fraude."""
    df = _get_df(request)
    total = len(df)
    total_fraud = int((df["is_fraud"] == 1).sum())
    return TransactionSummary(
        total_transactions=total,
        total_fraud=total_fraud,
        fraud_rate_pct=round(total_fraud / total * 100, 2),
        avg_transaction_value=round(float(df["amt"].mean()), 2),
        avg_fraud_value=round(float(df[df["is_fraud"] == 1]["amt"].mean()), 2),
    )


@router.get("/fraud/by-category", response_model=list[FraudByCategory])
def get_fraud_by_category(request: Request):
    """Retorna contagem e taxa de fraude agrupadas por categoria de transação."""
    df = _get_df(request)
    result = []
    for category, group in df.groupby("category"):
        total_in_cat = len(group)
        fraud_count = int((group["is_fraud"] == 1).sum())
        result.append(FraudByCategory(
            category=str(category),
            total_fraud=fraud_count,
            fraud_rate_pct=round(fraud_count / total_in_cat * 100, 2),
        ))
    return sorted(result, key=lambda x: x.total_fraud, reverse=True)


@router.get("/fraud/by-period", response_model=list[FraudByPeriod])
def get_fraud_by_period(request: Request):
    """Retorna contagem e taxa de fraude agrupadas por período do dia."""
    df = _get_df(request)
    result = []
    for period, group in df.groupby("transaction_period"):
        total_in_period = len(group)
        fraud_count = int((group["is_fraud"] == 1).sum())
        result.append(FraudByPeriod(
            period=str(period),
            total_fraud=fraud_count,
            fraud_rate_pct=round(fraud_count / total_in_period * 100, 2),
        ))
    return sorted(result, key=lambda x: x.total_fraud, reverse=True)
