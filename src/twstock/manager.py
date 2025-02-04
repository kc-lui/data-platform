import argparse
import logging
import requests
import dateutil.parser
from typing import List
from datetime import date, datetime

import pandas as pd

import twstock.models as models
from twstock.twse_client import TWSE


class Manager:
    def __init__(self):
        self.twse = TWSE()

    def _to_float(self, v: str) -> float:
        try:
            return float(v)
        except ValueError:
            return 0

    def _convert_minguo_to_ad(self, minguo_date: str) -> str:
        year, month, day = map(int, minguo_date.split('/'))
        ad_year = year + 1911
        return f"{ad_year}/{month:02d}/{day:02d}"

    def get_symbols(self) -> List[models.Symbol]:
        resp = self.twse.get_today_ohlcv()
        resp_pd = pd.DataFrame(resp)
        symbols = resp_pd[["Code", "Name"]]
        logging.debug(symbols)

        ret_symbols = []

        for _, item in symbols.iterrows():
            row = models.Symbol(
                code=item.get("Code"),
                name=item.get("Name"),
            )
            ret_symbols.append(row)

        return ret_symbols

    def get_ohlcv(self, symbol: str, start: datetime) -> List[models.DailyOHLCV]:
        resp = self.twse.get_ohlcv(symbol, start)
        logging.debug(resp)
        ret_ohlcv = []

        for entry in resp['data']:
            date_str, capacity, volume, open_price, high, low, close, change, transaction = entry

            date_obj = datetime.strptime(self._convert_minguo_to_ad(date_str), "%Y/%m/%d")
            capacity = int(capacity.replace(',', ''))
            volume = self._to_float(volume.replace(',', ''))
            open_price = self._to_float(open_price.replace(',', ''))
            high = self._to_float(high.replace(',', ''))
            low = self._to_float(low.replace(',', ''))
            close = self._to_float(close.replace(',', ''))
            change = self._to_float(change.replace(',', ''))
            transaction = int(transaction.replace(',', ''))

            row = models.DailyOHLCV(
                code=symbol,
                date=date_obj,
                open=open_price,
                high=high,
                low=low,
                close=close,
                capacity=capacity,
                volume=volume,
                change=change,
                transaction=transaction
            )
            ret_ohlcv.append(row)

        return ret_ohlcv

    def get_today_ohlcv(self) -> List[models.DailyOHLCV]:
        resp = self.twse.get_today_ohlcv()
        resp_pd = pd.DataFrame(resp).rename(
            columns={
                "TradeVolume": "Capacity",
                "TradeValue": "Volume",
                "OpeningPrice": "Open",
                "HighestPrice": "High",
                "LowestPrice": "Low",
                "ClosingPrice": "Close",
            },
        )
        resp_pd["Date"] = date.today()

        ohlcv = resp_pd.drop(columns=["Name"])
        logging.debug(ohlcv)

        ret_ohlcv = []

        for _, item in ohlcv.iterrows():
            row = models.DailyOHLCV(
                code=item.get("Code"),
                date=item.get("Date"),
                open=self._to_float(item.get("Open")),
                high=self._to_float(item.get("High")),
                low=self._to_float(item.get("Low")),
                close=self._to_float(item.get("Close")),
                capacity=self._to_float(item.get("Capacity")),
                volume=self._to_float(item.get("Volume")),
                change=self._to_float(item.get("Change")),
                transaction=self._to_float(item.get("Transaction")),
            )
            ret_ohlcv.append(row)

        return ret_ohlcv


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Manager Test")
    parser.add_argument(
        "--start", type=lambda d: dateutil.parser.parse(d), help="start time, inclusive"
    )
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["debug", "info", "warn", "error"],
        default="info",
        help="logging level, default info",
    )
    args = parser.parse_args()

    mapping = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARN,
        "error": logging.ERROR,
    }
    lv = logging.ERROR
    if args.log_level in mapping:
        lv = mapping[args.log_level]

    logging.basicConfig(
        level=lv,
        format="%(asctime)s %(levelname)s %(process)d %(filename)s %(lineno)d %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    logging.info("args: %s", args)

    mgr = Manager()
    mgr.get_symbols()
    mgr.get_today_ohlcv()
    mgr.get_ohlcv("2330", args.start)
