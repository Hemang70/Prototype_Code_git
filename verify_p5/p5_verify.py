#!/usr/bin/env python3
"""
p5_verify.py - differential test: P5 (VPAModel in k8s_prototype.py) vs the Kubernetes VPA 1.8.0 recommender
code (histogram + estimator + sample aggregation), compiled from the UNMODIFIED upstream files
(tag vertical-pod-autoscaler-1.8.0, commit 238579734e73) into ./vpacheck/vpacheck.

Both implementations receive the SAME 1-minute CPU samples at the SAME timestamps:
  * real VPA : original-scale samples (cores)
  * P5 port  : samples multiplied by `scale` (the prototype scales its traces down) + VPAModel(scale, ...)
P5 output is divided by `scale` before comparison, so a pass also verifies the "scale-corrected" claim.

Two comparison modes (the real VPA keeps CPU as INTEGER MILLICORES and truncates at every step):
  hires   : harness unit = nanocores (only the unit conversion differs from upstream) -> no truncation, so any
            difference is algorithmic.  Expect agreement to floating-point noise.
  shipped : harness unit = millicores exactly as upstream; both sides get the same whole-millicore samples.
            The result is floor()ed three times
            (percentile, margin, confidence multiplier); the largest possible difference is
                 port - real  in  [0, 2.15 * mult + 1] mCPU        (mult = confidence multiplier, 1 for target)
            which is derived below and checked for every query.

usage:  python3 p5_verify.py --traces traces.json --proto k8s_prototype.py --bin vpacheck/vpacheck
"""
import argparse, importlib.util, json, math, random, statistics, subprocess, sys

HIRES, MILLI = 1e9, 1000.0
NAMES = ('target', 'lower', 'upper')

def load_proto(path):
    spec = importlib.util.spec_from_file_location('k8s_prototype', path)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m

def run_real(binary, series, queries, unit):
    r = subprocess.run([binary], input=json.dumps({'samples': [[t, v] for t, v in series], 'queries': queries,
                                                   'unit': unit}), capture_output=True, text=True, check=True)
    return {round(x['T']): x for x in json.loads(r.stdout)}

def run_port(proto, scale, series):
    V = proto.VPAModel(scale, 5.0, []); out = {}
    for t, v in series:
        V.add(v * scale, t)
        lo, hi = V.bounds()
        out[round(t)] = (V.target() / scale * 1000, lo / scale * 1000, hi / scale * 1000)
    return out

def confidence_mult(series, upto_t):
    s = [(t, v) for t, v in series if t <= upto_t]
    if not s: return None
    c = min((s[-1][0] - s[0][0]) / 86400.0, len(s) / 1440.0)
    return c

def compare(proto, binary, scale, series, every=1):
    qs = [t for t, _ in series][::every]
    # shipped mode: the real VPA turns every sample into whole millicores (CPUAmountFromCores truncates), so the
    # port is given the identical truncated samples; hires mode feeds both sides the raw samples.
    trunc = [(t, int(v * 1000.0) / 1000.0) for t, v in series]
    ports = {'hires': run_port(proto, scale, series), 'shipped': run_port(proto, scale, trunc)}
    res = {}
    for mode, unit in (('hires', HIRES), ('shipped', MILLI)):
        port = ports[mode]
        real = run_real(binary, series, qs, unit)
        worst_rel = [0.0] * 3; viol = 0; worst_over = 0.0; n = 0; first_viol = None
        for t in qs:
            for i, k in enumerate(NAMES):
                raw = real[round(t)][k.capitalize()]; pv = port[round(t)][i]
                rv = raw * 1000.0 / unit                               # real VPA amount -> millicores
                real_inf = raw >= 9.9e13                               # MaxResourceAmount clamp = infinite upper bound
                if real_inf or math.isinf(pv):                         # no history -> infinite upper bound
                    if real_inf != math.isinf(pv): viol += 1
                    continue
                rd = abs(pv - rv) / max(rv, 1e-9)
                if mode == 'hires':
                    worst_rel[i] = max(worst_rel[i], rd)
                    if rd > 1e-6: viol += 1; first_viol = first_viol or (t, k, rv, pv)
                else:
                    c = confidence_mult(series, t)
                    mult = 1.0 if (i == 0 or not c) else ((1 + 1 / c) if i == 2 else (1 + 0.001 / c) ** -2)
                    bound = 2.15 * mult + 1.0
                    worst_rel[i] = max(worst_rel[i], rd)
                    d = pv - rv
                    worst_over = max(worst_over, d / bound)
                    if d < -1e-6 or d > bound + 1e-6: viol += 1; first_viol = first_viol or (t, k, rv, pv)
            n += 1
        res[mode] = dict(n=n, worst=worst_rel, viol=viol, used=worst_over, first=first_viol)
    return res

def series_from_trace(t):
    scale = t.get('scale', 1.0); per = max(int(round(60 / t['step_s'])), 1)
    seed = t['pooled_seed']; chunks = [seed[i:i + per] for i in range(0, len(seed), per)]
    s = [(-(len(chunks) - j) * 60.0, statistics.mean(c) / scale) for j, c in enumerate(chunks)]
    tr = t['cpu_trace']
    for k in range(per, len(tr) + 1, per):
        s.append((k / per * 60.0, statistics.mean(tr[k - per:k]) / scale))
    return scale, s

def synthetic(kind, seed, minutes, base):
    rnd = random.Random(seed); s = []; v = base
    for m in range(minutes):
        if kind == 'const': x = base
        elif kind == 'noise': x = base * (1 + rnd.gauss(0, 0.2))
        elif kind == 'burst': x = base * (6 if rnd.random() < 0.05 else 1) * (1 + rnd.gauss(0, 0.1))
        elif kind == 'step': x = base * (1 if m < minutes // 2 else 4)
        elif kind == 'diurnal': x = base * (1.5 + math.sin(2 * math.pi * m / 1440))
        elif kind == 'walk': v = max(0.001, v * math.exp(rnd.gauss(0, 0.05))); x = v
        elif kind == 'tiny': x = base * (1 + rnd.gauss(0, 0.5))
        s.append((m * 60.0, max(x, 0.0)))
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--traces', required=True); ap.add_argument('--proto', required=True); ap.add_argument('--bin', required=True)
    a = ap.parse_args(); proto = load_proto(a.proto)
    rows = []; bad = []

    def line(name, r):
        h, s = r['hires'], r['shipped']
        ok = h['viol'] == 0 and s['viol'] == 0
        print(f"{'ok  ' if ok else 'FAIL'} {name:30s} n={h['n']:4d} | hires worst rel: "
              + ' '.join(f"{x * 100:8.5f}%" for x in h['worst']) + f" | shipped: {s['viol']} outside truncation bound "
              f"(max {s['used'] * 100:3.0f}% of allowed)")
        if not ok:
            bad.append(name)
            for tag, m in (('hires', h), ('shipped', s)):
                if m['first']: print(f'       first {tag} violation: t={m["first"][0]} {m["first"][1]} real={m["first"][2]:.3f} port={m["first"][3]:.3f}')

    print('== A. the 28 workload windows in traces.json (prototype-scaled data vs real VPA on original scale)')
    print(f"{'':5s}{'trace':30s} {'':6s} | {'target':>9s} {'lower':>9s} {'upper':>9s}")
    for t in json.load(open(a.traces))['traces']:
        scale, s = series_from_trace(t)
        line(f"{t['key']} (scale {scale:.4f})", compare(proto, a.bin, scale, s))

    print('\n== B. 3-day synthetic histories, 1 sample/min (decay, confidence multipliers, bucket edges, minimum 25m)')
    for kind in ('const', 'noise', 'burst', 'step', 'diurnal', 'walk', 'tiny'):
        for scale, base in ((1.0, 0.4), (0.02, 0.4), (1.0, 0.012)):
            line(f'{kind} scale={scale} base={base}', compare(proto, a.bin, scale, synthetic(kind, 7, 3 * 1440, base), every=15))

    print('\nRESULT:', 'PASS' if not bad else f'{len(bad)} case(s) outside tolerance: {bad}')
    sys.exit(1 if bad else 0)

if __name__ == '__main__':
    main()
