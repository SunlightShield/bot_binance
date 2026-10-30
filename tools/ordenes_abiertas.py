import configparser
from binance.client import Client

cfg = configparser.ConfigParser()
cfg.read("user.cfg")
c = cfg["binance_user_config"]
cl = Client(c["api_key"], c["api_secret_key"], testnet=True)
o = cl.get_open_orders()
print("ORDENES ABIERTAS:", len(o))
for x in o:
    print(x["orderId"], x["symbol"], x["side"], x["status"])
