import asyncio
import aiohttp
import datetime as dt
import pandas as pd
import os
from src.rate_limiter import RateLimiter
import progressbar
from src.utils import *

class DataFetcher:
    def __init__(self, fetchintervall, loop=None, ratelimiter=None,
                 limit=100, symbol='BTC-USD', columns=None):
        self.limit = limit
        self.result = []
        self.tasks = []
        self.symbol = symbol
        self.since = None
        self.latest_candle = []
        self.columns = columns

        if loop is not None:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()

        if not ratelimiter:
            self.rate_lim = RateLimiter(0)
        else:
            self.rate_lim = ratelimiter

        self.now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).replace(second=0)
        self.since = self.now - fetchintervall

    async def download_once(self, client, start, end):
        params, call_url = self.get_call_wording(start, end)
        async with client.get(call_url, params=params) as resp:
            if resp.status == 200:
                fresh = await resp.text()
                fresh = self.preprocess(fresh)

                if fresh:
                    t = dt.datetime.utcfromtimestamp(fresh[0][0])
                    self.result += fresh
                    if int(start.timestamp()) != fresh[0][0]:
                        print(f'Wanted time {start}, but received {t}')
                else:
                    print(f"Empty Object received for time t={start}")
            else:
                print('REST API request went wrong when fetching data for time point t={} with HTTP status {}'
                      .format(start, resp.status))

    async def download_all(self):
        step = dt.timedelta(minutes=self.limit)
        stepm1 = dt.timedelta(minutes=self.limit-1)
        total_steps = (self.now - self.since)//step
        time_iterator = self.since
        print('Downloading:')
        async with aiohttp.ClientSession() as client:
            for i in progressbar.progressbar(range(total_steps), redirect_stdout=True):
                async with self.rate_lim(1.05):
                    self.tasks.append(self.loop.create_task(self.download_once(client, time_iterator,
                                                                               (time_iterator+stepm1))))
                    time_iterator += step
            await asyncio.gather(*self.tasks)

        self.dump_result()
        await self.rate_lim.end()

    def dump_result(self):
        print('Post-processing')
        self.sort()
        # missing = []
        # missingsmall = []
        # duplicates = []
        # for i in range(len(self.result) - 1):
        #     self.result[i][0] = round(self.result[i][0]/60)*60
        # for i in progressbar.progressbar(range(0, len(self.result) - 1), redirect_stdout=True):
        #     diff = self.result[i + 1][0] - self.result[i][0]
        #     if diff > 10 * 60000:
        #         t = self.result[i][0]
        #         missing += [t]
        #     elif diff > 60000:
        #         t = self.result[i][0]
        #         candle = [t + 60000] + self.result[i][1:-1] + [0]
        #         self.result.insert(i + 1, candle)
        #         missingsmall += [t]
        #     else:
        #         duplicates += [i]

        # for d in reversed(duplicates):
        #     del self.result[d]

        # print('Missing: ')
        # print(missing)
        # print(f"total amount of duplicates: {len(duplicates)}")
        pd_frame = pd.DataFrame(self.result, columns=self.columns)

        self.postprocess(pd_frame)
        self.write_file(pd_frame)

    def sort(self):
        self.result = sorted(self.result, key=lambda can: can[0])

        for i in range(0, len(self.result) - 1):
            if self.result[i + 1][0] < self.result[i][0]:
                print("sorting failed")
                break

    def write_file(self, pd_frame):
        datapath = get_project_root()/'Data'
        datapath.mkdir(parents=True, exist_ok=True)

        i = 0
        while (datapath/f'BTCUSD_ver{i}').exists():
            i += 1

        filename = datapath/f'BTCUSD_ver{i}'
        print(f"Writing to file {filename}")
        pd_frame.to_csv(filename, chunksize=2628000)

    def heal(self, fresh):
        if self.latest_candle:
            fresh = [self.latest_candle] + fresh

        for i in reversed(range(len(fresh) - 1)):
            diff = fresh[i + 1][0] - fresh[i][0]
            if 60000 < diff < 10 * 60000:
                t = fresh[i][0]
                candle = [t + 60000] + fresh[i][1:-1] + [0]
                self.result.insert(i + 1, candle)

        if self.latest_candle:
            del fresh[0]

    def preprocess(self, pdframe):
        pass

    def postprocess(self, pdframe):
        pass

    def get_call_wording(self, start, end):
        raise NotImplementedError
