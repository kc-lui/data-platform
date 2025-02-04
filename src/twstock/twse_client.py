import argparse
import logging
import requests
import dateutil.parser
from typing import Iterator, Optional
from datetime import datetime

class TWSE:
    def __init__(self):
        self.base_url = "https://openapi.twse.com.tw"
        self.timeout = 3
        self.retry_attemps = 2

        retry_strategy = requests.adapters.Retry(  # type: ignore
            total=self.retry_attemps,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            # method_whitelist=["HEAD", "GET", "OPTIONS"],
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _send_api(self, method: str, url: str, params: Optional[dict] = None) -> dict:
        resp = self.session.request(
            method=method,
            url=url,
            params=params,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        resp_json = resp.json()
        return resp_json  # type: ignore

    def get_symbols(self):
        resp = self._send_api("GET", f"{self.base_url}/v1/opendata//opendata/t187ap03_P")
        return resp

    def get_top_shareholder(self):
        ''' retrieve shareholder list which holds over 10%
        '''
        resp = self._send_api("GET", f"{self.base_url}/v1/opendata/t187ap02_L")
        return resp

    def get_ohlcv(self, stockNo: str, start: datetime):
        params = {
            "stockNo": stockNo,
            "date": start.strftime("%Y%m01"),
            "response": "json",
        }

        resp = self._send_api("GET", "https://www.twse.com.tw/exchangeReport/STOCK_DAY", params)
        return resp

    def get_today_ohlcv(self):
        resp = self._send_api("GET", f"{self.base_url}/v1/exchangeReport/STOCK_DAY_ALL")
        return resp

    def get_today_BWIBBU(self):
        resp = self._send_api("GET", f"{self.base_url}/v1/exchangeReport/BWIBBU_ALL")
        return resp


if __name__ == "__main__":
    parser = argparse.ArgumentParser("TWSE test")
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

    twse = TWSE()
    resp = twse.get_ohlcv("0050", args.start)
    logging.debug(resp)
    resp = twse.get_today_ohlcv()
    logging.debug(resp)
    resp = twse.get_top_shareholder()
    logging.debug(resp)
    resp = twse.get_today_BWIBBU()
    logging.debug(resp)
