import threading
from binance_trade_bot.binance_stream_manager import OrderGuard

m = threading.Lock()
pending = set()

g = OrderGuard(pending, m)
print("tomado tras crear:", m.locked())
g.release()
print("libre tras release:", not m.locked())

g = OrderGuard(pending, m)
g.set_order("DASH", "USDT", 1)
with g:
    pass
print("libre tras flujo normal:", not m.locked())
g.release()
print("release extra no rompe nada:", not m.locked())
