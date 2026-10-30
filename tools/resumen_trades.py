import json, sys
from datetime import datetime
from statistics import median

def p(s):
    return datetime.fromisoformat(s)

with open(sys.argv[1], encoding="utf-8") as f:
    rows = json.load(f)
rows.sort(key=lambda r: r["ts_utc"])

print("%-19s %-9s %-5s %-9s %10s %9s %9s %7s" % ("hora local", "par", "lado", "estado", "cantidad", "limite", "relleno", "seg"))
lat = []
for r in rows:
    seg = int((p(r["ts_utc"]) - p(r["created_utc"])).total_seconds())
    fill = "-" if r["avg_fill_price"] is None else "%.4f" % r["avg_fill_price"]
    print("%-19s %-9s %-5s %-9s %10g %9g %9s %7d" % (
        r["ts_local"], r["symbol"], r["side"], r["status"], r["orig_qty"], r["limit_price"], fill, seg))
    if r["status"] == "FILLED":
        lat.append(seg)

filled = [r for r in rows if r["status"] == "FILLED"]
canceled = [r for r in rows if r["status"] != "FILLED"]
print()
print("ordenes:", len(rows), "| llenadas:", len(filled), "| canceladas u otras:", len(canceled))
if lat:
    print("segundos hasta llenarse: mediana", median(lat), "| maximo", max(lat))

jumps = 0
for a, b in zip(rows, rows[1:]):
    if a["side"] == "SELL" and a["status"] == "FILLED" and b["side"] == "BUY" and b["status"] == "FILLED":
        jumps += 1
        gap = int((p(b["ts_utc"]) - p(a["ts_utc"])).total_seconds())
        print("salto: %s -> %s | venta y compra separadas por %d s" % (
            a["symbol"].replace("USDT", ""), b["symbol"].replace("USDT", ""), gap))
print("saltos completos (venta llenada + compra llenada):", jumps)
