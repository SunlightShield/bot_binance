import sqlite3, sys
from datetime import datetime
con = sqlite3.connect("file:" + sys.argv[1] + "?mode=ro", uri=True)
rows = [r[0] for r in con.execute("select distinct datetime from scout_history order by datetime")]
ts = [datetime.strptime(r[:19], "%Y-%m-%d %H:%M:%S") for r in rows]
gaps = [((b - a).total_seconds(), a, b) for a, b in zip(ts, ts[1:])]
print("ciclos distintos:", len(ts), "| primero:", ts[0], "| ultimo:", ts[-1])
print("huecos > 30 s:", sum(g[0] > 30 for g in gaps))
print("huecos > 60 s:", sum(g[0] > 60 for g in gaps))
print("huecos > 60 s, detalle:")
for g, a, b in sorted((g for g in gaps if g[0] > 60), reverse=True):
    print("  %7.1f s  %s -> %s" % (g, a, b))
