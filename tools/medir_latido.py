import re, sys
from datetime import datetime

pat = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+ .*job=updating value history")
ts = []
with open(sys.argv[1], encoding="utf-8", errors="replace") as f:
    for line in f:
        m = pat.match(line)
        if m:
            ts.append(datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S"))

if len(ts) < 2:
    print("pocas lineas de latido:", len(ts))
    sys.exit()

gaps = [((b - a).total_seconds(), a, b) for a, b in zip(ts, ts[1:])]
print("latidos:", len(ts), "| primero:", ts[0], "| ultimo:", ts[-1])
print("ultimo latido hace (s):", int((datetime.now() - ts[-1]).total_seconds()))
print("mayor hueco (s):", max(g[0] for g in gaps))
print("huecos > 90 s:", sum(g[0] > 90 for g in gaps), "| > 120 s:", sum(g[0] > 120 for g in gaps))
for g, a, b in sorted((g for g in gaps if g[0] > 90), reverse=True)[:10]:
    print("  %7.1f s  %s -> %s" % (g, a, b))
