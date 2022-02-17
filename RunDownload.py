import asyncio
import datetime

from src.data_fetcher_binance import DataFetcherBinance
from src.data_fetcher_coinbase import DataFetcherCoinbase


async def main():
    duration = datetime.timedelta(days=2)
    df = DataFetcherCoinbase(fetchintervall=duration)
    await df.download_all()

loop = asyncio.get_event_loop()

loop.run_until_complete(main())
loop.close()
