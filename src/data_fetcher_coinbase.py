from src.data_fetcher import DataFetcher
import json


class DataFetcherCoinbase(DataFetcher):
    def __init__(self, fetchintervall, loop=None, ratelimiter=None, limit=300, symbol='BTC-USD', columnsorder=None):
        super().__init__(fetchintervall, loop=loop, ratelimiter=ratelimiter,
                         limit=limit, symbol=symbol)
        if columnsorder is None:
            columnsorder = list('tLHOCV')
        self.columns = columnsorder

    def get_call_wording(self, start, end):
        params = {'start': start.isoformat(), 'end': end.isoformat(), 'granularity': 60}
        string = f"https://api.pro.coinbase.com/products/{self.symbol}/candles"
        return params, string

    def preprocess(self, fresh):
        fresh = json.loads(fresh)
        fresh.reverse()
        self.heal(fresh)
        return fresh
