import sqlite3, sys
from datetime import datetime

db = sys.argv[1]
con = sqlite3.connect("file:" + db + "?mode=ro", uri=True)
rows = [r[0] for r in con.execute(
    "select distinct datetime from coin_value where interval = 'MINUTELY' order by datetime"
)]

def p(s):
    for f in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(s, f)
        except ValueError:
            pass
    raise ValueError("formato de fecha no reconocido: " + repr(s))

ts = [p(r) for r in rows]
gaps = sorted(((b - a).total_seconds(), a, b) for a, b in zip(ts, ts[1:]))
print("filas por minuto:", len(ts), "| primera:", ts[0], "| ultima:", ts[-1])
print("mediana del intervalo (s):", gaps[len(gaps) // 2][0])
print("huecos > 90 s:", sum(g[0] > 90 for g in gaps))
print("huecos > 120 s:", sum(g[0] > 120 for g in gaps))
print("10 huecos mas grandes:")
for g, a, b in gaps[-10:][::-1]:
    print("  %7.1f s  %s -> %s" % (g, a, b))
