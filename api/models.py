from pydantic import BaseModel
from datetime import datetime

class StockData(BaseModel):
    symbol: str
    timestamp: datetime
    currentPrice: float
    previousClose: float
    open: float | None
    dayHigh: float
    dayLow: float
    volume: float | None = None
    dividendYield: float | None = None
    # Agrega todos los campos que necesites exponer

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }