from src.rate_limiter import RateLimiter
import asyncio
import time

tasks = []

async def task():
    await asyncio.sleep(3)
    print('work done')

async def main():
    rl = RateLimiter(3)
    for i in range(5):
        async with rl(1):
            tasks.append(loop.create_task(task()))
        
    group1 = asyncio.gather(*tasks)
    await group1

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

loop.close()