from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


SQLALCHEMY_DATABASE_URL = "sqlite:///./stock_data.db"

#Create a SQLAlehcmy engine, allows the use of the connection across multiple threads
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
#Create a session factory, transactions aren't automatically flushed until a query or commit is made
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

#Creates a base ckass for the ORM model
Base = declarative_base()

#Creates a database session to routes or functions that require it
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()