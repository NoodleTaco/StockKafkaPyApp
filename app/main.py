from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from . import models, stock_service
from .database import engine, get_db

#SQLAlchemy creates tables that are defined in our models but do not exist in the DB
#Base is a base class the model inherits from, create all scans all models that inherit from Base
models.Base.metadata.create_all(bind=engine) #SQLAlechmy will use 'engine' to (connected to DB) to create tables

#Create instance of FastAPI
app = FastAPI()

#get endpoint, pass in symbol, with the db parameter injected using FastAPI's di system
@app.get("/stock/{symbol}") 
def read_stock(symbol: str, db: Session = Depends(get_db)):
    #call stock service from the model
    stock_data = stock_service.get_stock_data(db, symbol=symbol)
    if stock_data is None:
        return {"error": "Stock data not found"}
    return {"symbol": stock_data.symbol, "price": stock_data.price, "moving_average": stock_data.moving_average, "timestamp": stock_data.timestamp}