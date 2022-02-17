# Download minutely BTCUSD ticker data
Downloads minutely BTCUSD tiker data of the last 2 days from coinbase or binance REST API and writes it to a json file in `Data/`.

Limits the amount of requests with asyncio according to a point system: 
- You have 30 points at start
- Every time you make a request, you get -1 points 
- After every second you gain 1 point
- You cannot have more than 30 points

## Running
Run `python3 RunDonwload.py` 
(`test.py` is an example for the `rate_limiter`)
