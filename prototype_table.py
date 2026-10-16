#!/usr/bin/env python3
"""
prototype_table.py — Section X-B results tables (run on Windows after git pull).

  python prototype_table.py --results results

Reads every results/*/summary.csv written by k8s_prototype.py:
  mode = cluster          Kubernetes runs (r1, r2, r3 ...)
  mode = offline-natural  the SAME windows replayed in the simulator environment
                          (k8s_prototype.py --offline --run sim)

Per dataset and set (representative A_00…A_09 / stress A_S0, A_S1):
  TABLE X-B-1  cluster: mean over pods, ± SD over runs (measurement window only)
               Request, Limit, Used, Over-req, Under-req, Unserved, Throttled %,
               Resizes/pod, Restarts
  TABLE X-B-2  simulator environment on the same windows (same columns)
  TABLE X-B-3  P6 vs each baseline in both environments: request reduction,
               throttle difference, pods where P6 requested less
  TABLE X-B-4  do the conclusions hold? For request, throttling and
               under-request: policy ranking in both environments, Kendall
               tau-b of the policy means, mean per-pod tau-b, and the share of
               (pod, P6-vs-baseline) comparisons whose direction is the same in
               both environments (differences inside a tolerance band count as ties).

Output: prototype_results.txt and prototype_results.csv in --results.
"""
import argparse, csv, glob, math, os, statistics
from collections import defaultdict

ORDER = ['P1', 'P2', 'P3', 'P5', 'P6', 'P7', 'P8']
NAME = {'P5': 'K8s VPA 1.8.0', 'P1': 'Peak+15% Over-Prov.', 'P2': 'VPA-Reactive', 'P3': 'SHOWAR',
        'P6': 'Burstiness-Adaptive', 'P7': 'Percentile-Only (ablation)', 'P8': 'Headroom-Only (ablation)'}
COLS = [('avg_req_mcpu', 'Request (m)'), ('avg_lim_mcpu', 'Limit (m)'),
        ('avg_eff_mcpu', 'Used (m)'), ('over_req_mcpu', 'Over-req (m)'),
        ('under_req_mcpu', 'Under-req (m)'), ('unserved_mcpu', 'Unserved (m)'),
        ('throttle_pct', 'Throttled (%)'), ('resizes', 'Resizes/pod'),
        ('restarts', 'Restarts')]
NUM = [c for c, _ in COLS] + ['avg_actual_mcpu', 'missed_samples', 'meas_steps']
# agreement metrics: (column, label, tie band) - band is absolute (m or pp)
AGREE = [('avg_req_mcpu', 'Request', lambda d: 0.02 * d),        # 2 % of mean demand
         ('throttle_pct', 'Throttled %', lambda d: 1.2),         # ~1 step of 84
         ('under_req_mcpu', 'Under-req', lambda d: 0.02 * d)]

def tau_b(x, y):
    """Kendall tau-b (ties allowed). None if undefined."""
    c = d = tx = ty = 0
    n = len(x)
    for i in range(n):
        for j in range(i + 1, n):
            a = (x[i] > x[j]) - (x[i] < x[j]); b = (y[i] > y[j]) - (y[i] < y[j])
            if a == 0 and b == 0: continue
            if a == 0: tx += 1
            elif b == 0: ty += 1
            elif a == b: c += 1
            else: d += 1
    den = math.sqrt((c + d + tx) * (c + d + ty))
    return None if den == 0 else (c - d) / den

def sign(v, band):
    return 0 if abs(v) <= band else (1 if v > 0 else -1)

LEGACY = {'P5': 'P6', 'P6': 'P7', 'P7': 'P8', 'P8': 'P5'}   # code IDs before the relabel -> paper labels

def load(results, skip, legacy=frozenset()):
    rows, old = [], 0
    for f in glob.glob(os.path.join(results, '*', 'summary.csv')):
        with open(f, newline='', encoding='utf-8') as fh:
            for r in csv.DictReader(fh):
                if 'mode' not in r or 'under_req_mcpu' not in r:
                    old += 1; continue                       # v1 controller output
                if r['run'] in skip or float(r.get('meas_steps') or 0) == 0: continue
                if r['run'] in legacy: r['policy'] = LEGACY.get(r['policy'], r['policy'])
                for c in NUM: r[c] = float(r.get(c) or 0)
                r['set'] = r.get('set') or ('stress' if '_S' in r['key'] else 'representative')
                r['group_label'] = f"{r['dataset']} ({r['set']})"
                rows.append(r)
    return rows, old

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--results', default='results')
    ap.add_argument('--skip', default='smoke', help='comma-separated run labels to ignore')
    ap.add_argument('--sim-mode', default='offline-natural',
                    help='mode used as the simulator reference (default offline-natural)')
    ap.add_argument('--legacy-runs', default='', help='run labels produced BEFORE the relabel (old code IDs, e.g. r1); '
                    'their policy IDs are converted to the paper labels')
    a = ap.parse_args()
    rows, old = load(a.results, set(a.skip.split(',')), set(filter(None, a.legacy_runs.split(','))))
    if not rows: raise SystemExit(f'No v2 results found in {a.results}/*/summary.csv')
    cl = [r for r in rows if r['mode'] == 'cluster']
    sm = [r for r in rows if r['mode'] == a.sim_mode]

    L, out = [], []
    P = L.append
    runs = sorted({r['run'] for r in cl})
    P('KUBERNETES PROTOTYPE RESULTS (Section X-B)')
    P(f'cluster runs: {", ".join(runs) or "none"}   simulator-environment rows: {len(sm)}'
      f'   missed cgroup samples: {int(sum(r["missed_samples"] for r in cl))}')
    cfgs = {(r['censor_bump'], r['censor_mode'], r.get('censor_base', '')) for r in rows}
    P(f'censoring settings found: {sorted(cfgs)}' + ('   [WARN] mixed settings' if len(cfgs) > 1 else ''))
    if old: P(f'[note] {old} rows from the old controller (no mode/under_req columns) ignored')

    def table(title, data, with_sd):
        for gl in sorted({r['group_label'] for r in data}):
            d = [r for r in data if r['group_label'] == gl]
            keys = sorted({r['key'] for r in d}); rn = sorted({r['run'] for r in d})
            pols = [p for p in ORDER if any(r['policy'] == p for r in d)]
            P(f'\n{title} — Dataset {gl}: {len(keys)} pods × {len(pols)} policies × {len(rn)} run(s)')
            P(f'{"Policy":<26}' + ''.join(f'{lab:>16}' for _, lab in COLS))
            P('─' * (26 + 16 * len(COLS)))
            for p in pols:
                cells = []
                for c, _ in COLS:
                    per_run = [statistics.mean(r[c] for r in d if r['policy'] == p and r['run'] == run)
                               for run in rn if any(r['policy'] == p and r['run'] == run for r in d)]
                    m = statistics.mean(per_run)
                    sd = statistics.stdev(per_run) if len(per_run) > 1 else 0.0
                    if c == 'restarts': cells.append(f'{m:>9.0f}      ')
                    elif with_sd:       cells.append(f'{m:>8.1f} ± {sd:<5.1f}')
                    else:               cells.append(f'{m:>9.1f}      ')
                    out.append(dict(table=title, dataset=gl, policy=p, metric=c,
                                    mean=round(m, 3), sd_runs=round(sd, 3),
                                    runs=len(per_run), pods=len(keys)))
                P(f'{p + " " + NAME[p]:<26}' + ''.join(f'{x:>16}' for x in cells))

    def pod_means(data):
        """(group, key, policy) -> {metric: mean over runs}; group -> mean demand."""
        acc = defaultdict(list)
        for r in data: acc[(r['group_label'], r['key'], r['policy'])].append(r)
        return {k: {c: statistics.mean(x[c] for x in v) for c in NUM} for k, v in acc.items()}

    if cl: table('TABLE X-B-1  Kubernetes cluster', cl, True)
    if sm: table('TABLE X-B-2  Simulator environment, same windows', sm, False)

    # ---- P6 vs baselines, both environments
    envs = [('cluster', pod_means(cl)), ('simulator', pod_means(sm))]
    groups = sorted({k[0] for _, pm in envs for k in pm})
    P('\nTABLE X-B-3  P6 vs baselines (mean over pods; pods averaged over runs)')
    for gl in groups:
        P(f'\n  Dataset {gl}')
        for env, pm in envs:
            keys = sorted({k[1] for k in pm if k[0] == gl})
            if not keys or not any((gl, k, 'P6') in pm for k in keys): continue
            for b in [p for p in ORDER if p != 'P6' and any((gl, k, p) in pm for k in keys)]:
                pk = [k for k in keys if (gl, k, 'P6') in pm and (gl, k, b) in pm]
                if not pk: continue
                r5 = statistics.mean(pm[(gl, k, 'P6')]['avg_req_mcpu'] for k in pk)
                rb = statistics.mean(pm[(gl, k, b)]['avg_req_mcpu'] for k in pk)
                t5 = statistics.mean(pm[(gl, k, 'P6')]['throttle_pct'] for k in pk)
                tb = statistics.mean(pm[(gl, k, b)]['throttle_pct'] for k in pk)
                u5 = statistics.mean(pm[(gl, k, 'P6')]['under_req_mcpu'] for k in pk)
                ub = statistics.mean(pm[(gl, k, b)]['under_req_mcpu'] for k in pk)
                wins = sum(pm[(gl, k, 'P6')]['avg_req_mcpu'] < pm[(gl, k, b)]['avg_req_mcpu'] for k in pk)
                red = (1 - r5 / rb) * 100 if rb else 0.0
                P(f'    {env:<9} vs {b}: request {red:6.1f} % lower   throttled {t5 - tb:+6.2f} pp   '
                  f'under-req {u5 - ub:+7.1f} m   P6 requested less in {wins}/{len(pk)} pods')
                out.append(dict(table='X-B-3', dataset=gl, policy='P6', metric=f'{env}_vs_{b}',
                                mean=round(red, 3), throttle_diff_pp=round(t5 - tb, 3),
                                under_diff_m=round(u5 - ub, 3), pods_p5_lower=wins, pods=len(pk)))

    # ---- agreement between environments
    if cl and sm:
        pc, ps = envs[0][1], envs[1][1]
        P('\nTABLE X-B-4  Do the conclusions hold on the cluster? (cluster vs simulator environment)')
        P('  ranking = policies from lowest to highest mean;  tau = Kendall tau-b of the policy means;')
        P('  pod tau = mean per-pod tau-b;  direction = share of (pod, P6 vs baseline) comparisons')
        P('  with the same sign in both environments (|difference| within the band = tie)')
        for gl in groups:
            keys = sorted({k[1] for k in pc if k[0] == gl} & {k[1] for k in ps if k[0] == gl})
            pols = [p for p in ORDER if all((gl, k, p) in pc and (gl, k, p) in ps for k in keys)]
            if not keys or len(pols) < 2: continue
            dem = statistics.mean(pc[(gl, k, pols[0])]['avg_actual_mcpu'] for k in keys)
            P(f'\n  Dataset {gl}: {len(keys)} pods, policies {",".join(pols)}, band on mean demand {dem:.0f} m')
            P(f'  {"Metric":<13} {"Cluster ranking":<22} {"Simulator ranking":<22} {"tau":>6}'
              f' {"pod tau":>8} {"direction":>14}')
            for c, lab, bandf in AGREE:
                mc = [statistics.mean(pc[(gl, k, p)][c] for k in keys) for p in pols]
                ms = [statistics.mean(ps[(gl, k, p)][c] for k in keys) for p in pols]
                rk = lambda m: '<'.join(p for _, p in sorted(zip(m, pols)))
                t = tau_b(mc, ms)
                pt = [tau_b([pc[(gl, k, p)][c] for p in pols], [ps[(gl, k, p)][c] for p in pols])
                      for k in keys]
                pt = [x for x in pt if x is not None]
                band = bandf(dem); same = tot = 0
                if 'P6' in pols:
                    for k in keys:
                        for b in pols:
                            if b == 'P6': continue
                            dc = pc[(gl, k, 'P6')][c] - pc[(gl, k, b)][c]
                            ds = ps[(gl, k, 'P6')][c] - ps[(gl, k, b)][c]
                            tot += 1; same += sign(dc, band) == sign(ds, band)
                dirs = f'{same}/{tot} ({same / tot * 100:.0f}%)' if tot else '-'
                P(f'  {lab:<13} {rk(mc):<22} {rk(ms):<22} '
                  f'{("%.2f" % t) if t is not None else "  -":>6} '
                  f'{("%.2f" % statistics.mean(pt)) if pt else "  -":>8} {dirs:>14}')
                out.append(dict(table='X-B-4', dataset=gl, policy='all', metric=c,
                                mean=round(t, 3) if t is not None else '',
                                pod_tau=round(statistics.mean(pt), 3) if pt else '',
                                direction_agree=same, direction_total=tot, pods=len(keys),
                                rank_cluster=rk(mc), rank_sim=rk(ms)))
    elif cl:
        P('\n[note] no simulator-environment rows: run  python3 k8s_prototype.py --offline --run sim'
          '  (or on Windows) to get Table X-B-4')

    txt = os.path.join(a.results, 'prototype_results.txt')
    with open(txt, 'w', encoding='utf-8') as fh: fh.write('\n'.join(L) + '\n')
    cols = ['table', 'dataset', 'policy', 'metric', 'mean', 'sd_runs', 'runs', 'pods',
            'throttle_diff_pp', 'under_diff_m', 'pods_p5_lower', 'pod_tau',
            'direction_agree', 'direction_total', 'rank_cluster', 'rank_sim']
    with open(os.path.join(a.results, 'prototype_results.csv'), 'w', newline='', encoding='utf-8') as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator='\n'); w.writeheader(); w.writerows(out)
    print('\n'.join(L)); print(f'\nSaved: {txt}')

if __name__ == '__main__':
    main()
