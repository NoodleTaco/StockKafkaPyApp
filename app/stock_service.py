from sqlalchemy.orm import Session
from . import models
from datetime import datetime


#Creates a new record of stock data in DB
def create_stock_data(db: Session, symbol: str, price: float, moving_average: float, timestamp: datetime):

    #Create new instance of StockData model with provided params
    db_stock_data = models.StockData(symbol=symbol, price=price, moving_average=moving_average, timestamp=timestamp)
    #Add data to current DB session
    db.add(db_stock_data)
    #Commit transaction
    db.commit()
    db.refresh(db_stock_data)
    return db_stock_data

#Retrieves most recent stock data given a symbol
def get_stock_data(db: Session, symbol: str):
    return db.query(models.StockData).filter(models.StockData.symbol == symbol).order_by(models.StockData.timestamp.desc()).first()