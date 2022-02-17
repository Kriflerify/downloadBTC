from src.data_fetcher import DataFetcher
import json
import pandas as pd


class DataFetcherBinance(DataFetcher):
    def __init__(self, fetchintervall, loop=None, ratelimiter=None, limit=1000, symbol='BTCUSDT', columnsorder=None):
        if columnsorder is None:
            columnsorder = list('tOHLCV')
        super().__init__(fetchintervall, loop=loop, ratelimiter=ratelimiter,
                         limit=limit, symbol=symbol, columns=columnsorder)

    def get_call_wording(self, start, end):
        params = {'symbol': self.symbol, 'interval': '1m', 'startTime': int(start.timestamp())*1000,
                  'endTime': int(end.timestamp())*1000, 'limit': 1000}
        string = f"https://api.binance.com/api/v3/klines"
        return params, string

    # is applied immediately after receiving the answer from the GET request
    def preprocess(self, fresh):
        fresh = json.loads(fresh)
        fresh = [[int(x[0])//1000] + [float(i) for i in x[1:6]] for x in fresh]
        self.heal(fresh)
        return fresh

    def postprocess(self, pdframe):
        for c in list('OHLCV'):
            pdframe[c] = pd.to_numeric(pdframe[c], downcast='float')
