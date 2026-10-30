import sqlite3, sys
from collections import Counter
from datetime import datetime
con = sqlite3.connect("file:" + sys.argv[1] + "?mode=ro", uri=True)
print("filas por interval:", con.execute("select interval, count(distinct datetime) from coin_value group by interval").fetchall())
rows = [r[0] for r in con.execute("select distinct datetime from coin_value order by datetime")]
ts = [datetime.strptime(r[:19], "%Y-%m-%d %H:%M:%S") for r in rows]
gaps = [int((b - a).total_seconds()) for a, b in zip(ts, ts[1:])]
print("timestamps distintos:", len(ts))
print("histograma de intervalos (s, redondeado a 5):")
for k, v in sorted(Counter(5 * round(g / 5) for g in gaps).items()):
    if v >= 3: print("  %5d s: %d" % (k, v))
