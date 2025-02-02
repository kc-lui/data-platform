from sqlalchemy import Column, Integer, String, Float, DateTime, Index, func
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Symbol(Base):
    __tablename__ = "symbols"

    code = Column(String(50), primary_key=True, nullable=False)
    name = Column(String(50), nullable=False, server_default="")
    created_at = Column(DateTime, server_default=func.now())

class DailyOHLCV(Base):
    __tablename__ = "daily_ohlcv"

    code = Column(String(50), primary_key=True, nullable=False)
    date = Column(DateTime, primary_key=True, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    capacity = Column(Integer, nullable=False)
    volume = Column(Float, nullable=False)
    change = Column(Float, nullable=False)
    transaction = Column(Integer, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
