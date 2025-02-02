import json
from enum import Enum


class Team(Enum):
    operator = "operator"
    data = "data"

    def __str__(self):
        return self.value


class Major(Enum):
    example = "example"
    maintenance = "airflow-maintenance"
    marketdata = "marketdata"

    def __str__(self):
        return self.value

class Marketdata(Enum):
    symbols = "symbols"
    ohlcv = "ohlcv"
    market_calendar = "market_calendar"
    capm = "capm"

    def __str__(self):
        return self.value


class Queue(Enum):
    default = "default"
    kubernetes = "kubernetes"

    def __str__(self):
        return self.value


def create_pool_enum(file_path="pools.json"):
    pool_members = {"default": "default_pool"}
    with open(file_path, "r") as file:
        pools_data = json.load(file)
    for pool_name in pools_data.keys():
        pool_members[pool_name] = pool_name
    return Enum("Pool", pool_members)


Pool = create_pool_enum()

def create_connection_enum(file_path="connections.json"):
    members = {}
    with open(file_path, "r") as file:
        data = json.load(file)
    for name in data.keys():
        members[name] = name
    return Enum("Connection", members)


Connection = create_connection_enum()
