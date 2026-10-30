"""Cruza huecos de latido con esperas de orden. Solo lee el log: python tools/esperas_log.py RUTA_LOG [HORA_DESDE]"""
import re
import sys
from datetime import datetime

F = "%Y-%m-%d %H:%M:%S"
LINE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d{3} - \S+ - (\w+) - (.{0,200})")
BEAT = re.compile(r"job=updating value history duracion=([\d.]+)s")
SCOUT = re.compile(r"job=scouting duracion=([\d.]+)s")
WAIT = re.compile(r"Waiting for order (\d+) to be (filled|created)")

desde = datetime.strptime(sys.argv[2], F) if len(sys.argv) > 2 else None
starts, beats, scouts, waits, cancels, jumps, bad = [], [], [], {}, [], [], []
with open(sys.argv[1], encoding="utf-8", errors="replace") as f:
    for line in f:
        m = LINE.match(line)
        if not m:
            continue
        ts = datetime.strptime(m.group(1), F)
        if desde and ts < desde:
            continue
        lvl, msg = m.group(2), m.group(3)
        if msg == "Starting":
            starts.append(ts)
        elif BEAT.search(msg):
            beats.append((ts, float(BEAT.search(msg).group(1))))
        elif SCOUT.search(msg):
            scouts.append((ts, float(SCOUT.search(msg).group(1))))
        elif WAIT.search(msg):
            w = WAIT.search(msg)
            k = (w.group(1), w.group(2))
            d = waits.setdefault(k, {"first": ts, "last": ts, "n": 0})
            d["last"] = ts
            d["n"] += 1
        elif "Order timeout, canceled" in msg:
            cancels.append(ts)
        elif "Will be jumping" in msg:
            jumps.append((ts, msg))
        if lvl in ("WARNING", "ERROR", "CRITICAL"):
            bad.append((ts, lvl, msg[:120]))

print(f"Starting: {len(starts)} | latidos: {len(beats)} | saltos en log: {len(jumps)} | cancelaciones por timeout: {len(cancels)}")
print(f"WARNING/ERROR/CRITICAL: {len(bad)}")
for t, l, m in bad[:5]:
    print("  ", t.strftime(F), l, m)

print("\nESPERAS DE ORDEN (segundos entre la primera y la ultima linea 'Waiting for order'):")
ws = sorted(waits.items(), key=lambda kv: kv[1]["first"])
for (oid, kind), d in ws:
    s = (d["last"] - d["first"]).total_seconds()
    flag = "  <-- MAS DE 300 s" if s > 300 else ""
    print(f"  {d['first'].strftime(F)} orden {oid} ({kind}) {s:6.0f} s  n={d['n']}{flag}")
largas = [k for k, d in ws if (d["last"] - d["first"]).total_seconds() > 300]
print(f"esperas > 300 s: {len(largas)} de {len(ws)}")

print("\nHUECOS > 120 s ENTRE LATIDOS:")
cuel = esp = 0
for (t0, _), (t1, _) in zip(beats, beats[1:]):
    g = (t1 - t0).total_seconds()
    if g <= 120:
        continue
    if any(t0 < s <= t1 for s in starts):
        tipo = "reinicio"
    elif any(d["first"] <= t1 and d["last"] >= t0 for _, d in ws):
        tipo = "espera_de_orden"
        esp += 1
    else:
        tipo = "CUELGUE"
        cuel += 1
    print(f"  {g:6.0f} s  {t0.strftime(F)} -> {t1.strftime(F)}  {tipo}")
print(f"huecos: espera_de_orden={esp} cuelgues={cuel}")

lentos = [(t, d) for t, d in beats if d > 5]
print(f"\nupdating > 5 s: {len(lentos)} | scouting logueados (>2 s): {len(scouts)} | scouting > 10 s: {sum(1 for _, d in scouts if d > 10)}")
for t, d in scouts[:10]:
    print(f"  {t.strftime(F)} scouting {d} s")