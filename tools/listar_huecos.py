import sqlite3, sys
from datetime import datetime
con = sqlite3.connect("file:" + sys.argv[1] + "?mode=ro", uri=True)
rows = [r[0] for r in con.execute("select distinct datetime from coin_value where interval = 'MINUTELY' order by datetime")]
ts = [datetime.strptime(r[:19], "%Y-%m-%d %H:%M:%S") for r in rows]
for a, b in zip(ts, ts[1:]):
    g = (b - a).total_seconds()
    if g > 120:
        print("%7.1f s  %s -> %s" % (g, a, b))
