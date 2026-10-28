import asyncio
import configparser

import websockets
from binance.client import Client

# Primero el Client, antes de que asyncio.run cierre el event loop
cfg = configparser.ConfigParser()
cfg.read("user.cfg")
c = cfg["binance_user_config"]
client = Client(c["api_key"], c["api_secret_key"], testnet=True)


async def probe(name, url):
    try:
        async with websockets.connect(url) as ws:
            msg = await asyncio.wait_for(ws.recv(), timeout=10)
            print(f"{name}: OK ({len(msg)} bytes)")
    except Exception as e:
        print(f"{name}: FALLA -> {type(e).__name__}: {e}")


async def main():
    await probe("testnet miniTicker (host viejo)", "wss://testnet.binance.vision/ws/!miniTicker@arr")
    await probe("testnet miniTicker (host nuevo)", "wss://stream.testnet.binance.vision/ws/!miniTicker@arr")
    await probe("real miniTicker", "wss://stream.binance.com:9443/ws/!miniTicker@arr")


asyncio.run(main())

try:
    client.stream_get_listen_key()
    print("testnet listenKey: OK")
except Exception as e:
    print(f"testnet listenKey: FALLA -> {type(e).__name__}: {e}")