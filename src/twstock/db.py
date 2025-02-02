from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, Session, sessionmaker
from sqlalchemy.schema import CreateTable

import twstock.models as models

DATABASE_URL = "postgresql://airflow:airflow@localhost:5432/trade"


def get_session(uri: str) -> Session:
    engine = create_engine(uri)

    return sessionmaker(bind=engine)()


if __name__ == "__main__":
    engine = create_engine(DATABASE_URL, echo=True)
    print(CreateTable(models.Symbol.__table__).compile(engine))
    print(CreateTable(models.DailyOHLCV.__table__).compile(engine))

    Base = declarative_base()
    Base.metadata.create_all(bind=engine)
