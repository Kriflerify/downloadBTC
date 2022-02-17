import collections
import asyncio
from contextlib import suppress


class RateLimiter:
    def __init__(self, value=1, *, loop=None):
        if value < 0:
            raise ValueError("RateLimiter initial value must be >= 0")
        self._value = value  # The amount of tokens available
        self._waiters = collections.deque()
        if loop is not None:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        self.maxvalue = value
        self.incrementing = False
        self.incrementing_task = None
        self.start_incrementing()

    def start_incrementing(self):
        if not self.incrementing:
            self.incrementing = True
            self.incrementing_task = self._loop.create_task(self._increment())

    async def end(self):
        if self.incrementing:
            self.incrementing = False
            self.incrementing_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.incrementing_task

    def __call__(self, next_cost=3):
        self._next_cost = next_cost
        return self

    async def __aenter__(self):
        await self.acquire(self._next_cost)
        return None

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def acquire(self, cost):
        while self._value < cost:
            fut = self._loop.create_future()
            self._waiters.append((cost, fut))
            try:
                await fut
            except:
                fut.cancel()
                if self._value >= cost and not fut.cancelled():
                    self._wake_up_next()
                raise
        self._value -= cost
        # print('RateLimiter value: {}'.format(self._value))
        self.start_incrementing()
        return True

    def _wake_up_next(self):
        while self._waiters:
            waiter = self._waiters[-1]
            if not waiter[1].done():
                if waiter[0] <= self._value:
                    waiter[1].set_result(None)
                    self._waiters.popleft()
                return
            self._waiters.popleft()

    async def _increment(self):
        while True:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                return
            self._value += 1
            if self._value == self.maxvalue:
                # await self.stop_incrementing()
                self.incrementing = False
                self._wake_up_next()
                return
            # print('RateLimiter value: {}'.format(self._value))
            self._wake_up_next()
