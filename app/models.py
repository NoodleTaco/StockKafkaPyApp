from sqlalchemy import Column, Integer, String, Float, DateTime
from .database import Base

class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    price = Column(Float)
    moving_average = Column(Float)
    timestamp = Column(DateTime)