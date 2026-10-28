import json, urllib.request

coins = open("supported_coin_list").read().split()

def get(url):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.load(r)

info = {s["symbol"]: s for s in get("https://api.binance.com/api/v3/exchangeInfo")["symbols"]}
book = {b["symbol"]: b for b in get("https://api.binance.com/api/v3/ticker/bookTicker")}

rows = []
for c in coins:
    sym = c + "USDT"
    if sym not in info:
        rows.append((sym, None, None, None, "NO EXISTE"))
        continue
    tick = float(next(f["tickSize"] for f in info[sym]["filters"] if f["filterType"] == "PRICE_FILTER"))
    bid, ask = float(book[sym]["bidPrice"]), float(book[sym]["askPrice"])
    mid = (bid + ask) / 2
    rows.append((sym, tick / mid * 100, (ask - bid) / mid * 100, mid, info[sym]["status"]))

print(f"{'par':10} {'tick %':>8} {'spread %':>9} {'precio':>12}  estado")
for r in sorted(rows, key=lambda r: -(r[1] or 0)):
    if r[1] is None:
        print(f"{r[0]:10} {'-':>8} {'-':>9} {'-':>12}  {r[4]}")
    else:
        print(f"{r[0]:10} {r[1]:8.4f} {r[2]:9.4f} {r[3]:12.6f}  {r[4]}")
