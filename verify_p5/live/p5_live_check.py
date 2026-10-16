#!/usr/bin/env python3
"""
p5_live_check.py - compare P5 (VPAModel in k8s_prototype.py) with the REAL Kubernetes VPA 1.8.0 recommender
running in your cluster.  Standard library only; needs kubectl.

  python3 p5_live_check.py install                     # metrics-server (if missing) + VPA 1.8.0 CRDs/RBAC/recommender
  python3 p5_live_check.py run --minutes 35            # replay a CPU pattern, record VPA + metrics-server every 5 s
  python3 p5_live_check.py compare live_run_XXXX       # Check 1 (exact) and Check 2 (end-to-end)
  python3 p5_live_check.py cleanup                     # remove the test workload (add --all to remove VPA too)
  python3 p5_live_check.py selftest --bin ./vpacheck-linux-amd64   # test the comparison logic offline (no cluster)

Check 1 (exact): the recommender stores its CPU histogram in a VerticalPodAutoscalerCheckpoint object.  We load THAT
   histogram into P5's VPAModel and ask P5 for target / lower / upper.  The result must equal what the real VPA
   published in the VPA object status (within VPA's own integer-millicore rounding and the checkpoint's
   10000-step weight rounding).  This removes all sampling-time differences: same state in, same numbers out.
Check 2 (end-to-end, approximate): P5 is fed the usage values reported by metrics-server at the times the recommender
   recorded its samples.  The recommender picks its own sampling instants, so agreement is expected within about one
   histogram bucket (5 %), not exactly.
"""
import argparse, datetime as dt, importlib.util, json, math, os, random, re, subprocess, sys, time

NS, DEPLOY, VPA_NAME, CM = 'default', 'vpa-ref', 'p5-ref', 'vpa-ref-replayer'
HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------- helpers
def kubectl(*a, inp=None, check=True):
    r = subprocess.run(['kubectl', *a], input=inp, capture_output=True, text=True)
    if check and r.returncode: sys.exit(f'[ERROR] kubectl {" ".join(a)}\n{r.stderr.strip()}')
    return r

def kjson(*a):
    r = kubectl(*a, '-o', 'json', check=False)
    return json.loads(r.stdout) if r.returncode == 0 and r.stdout.strip() else None

def load_proto(path):
    spec = importlib.util.spec_from_file_location('k8s_prototype', path)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m

def ts(s):                                   # RFC3339 -> epoch seconds
    return dt.datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp()

def iso(t):
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def cpu_of(x):                               # VPA status values are ResourceLists: {'cpu': '25m', 'memory': ...}
    return x['cpu'] if isinstance(x, dict) else x

def q_mcpu(q):                               # Kubernetes quantity -> millicores (n u m, k M G T P E)
    q = str(q); m = re.fullmatch(r'([0-9.]+)([numkMGTPE]?)', q)
    if not m: raise ValueError(q)
    return float(m.group(1)) * {'n': 1e-6, 'u': 1e-3, 'm': 1.0, '': 1000.0, 'k': 1e6, 'M': 1e9,
                                'G': 1e12, 'T': 1e15, 'P': 1e18, 'E': 1e21}[m.group(2)]

# ---------------------------------------------------------------- P5 on a loaded VPA state
def estimate(V, dq=0.0):
    """P5's OWN target() / bounds() in millicores for histogram state V.  dq shifts every percentile query by dq
    (used only to allow for the rounding of the weights stored in the checkpoint).  The returned `mult` is the
    reference VPA confidence multiplier, used only to size the integer-truncation tolerance."""
    orig = V.pct
    V.pct = lambda q: orig(min(max(q + dq, 0.0), 1.0))
    try:
        t = V.target(); lo, hi = V.bounds()
    finally:
        V.pct = orig
    c = min((V.last_t - V.first_t) / 86400.0, V.count / 1440.0) if V.count else 0.0
    mult = (1.0, (1 + 0.001 / c) ** -2.0 if c > 0 else 1.0, (1 + 1.0 / c) if c > 0 else float('inf'))
    return (t * 1000, lo * 1000, hi * 1000), mult

def model_from_checkpoint(proto, ck):
    st = ck['status']; h = st['cpuHistogram']
    bw = {int(k): float(v) for k, v in (h.get('bucketWeights') or {}).items()}
    V = proto.VPAModel(1.0, 5.0, [])
    s = sum(bw.values()); tw = float(h['totalWeight'])
    for i, w in bw.items(): V.w[i] = w * tw / s            # checkpoint stores weights scaled so that max = 10000
    V.total = sum(V.w); V.count = int(st['totalSamplesCount'])
    V.first_t = 0.0; V.last_t = ts(st['lastSampleStart']) - ts(st['firstSampleStart'])
    delta = 0.5 * len(bw) / s                              # worst-case cumulative rounding of the stored weights
    return V, delta

def check1(proto, ck, rec):
    """Return (ok, details) for one (checkpoint, recommendation) pair."""
    V, delta = model_from_checkpoint(proto, ck)
    base, mult = estimate(V)
    lo_v, _ = estimate(V, -delta); hi_v, _ = estimate(V, +delta)
    rows, ok = [], True
    for i, k in enumerate(('target', 'lowerBound', 'upperBound')):
        real = q_mcpu(cpu_of(rec[k])); lo_e = lo_v[i]; hi_e = hi_v[i]
        if real >= 1e12 and (math.isinf(base[i]) or math.isinf(mult[i]) or mult[i] > 100.0):
            good = True; rows.append((k, real, base[i], good)); ok &= good; continue
        if math.isinf(base[i]):
            good = real >= 1e9; rows.append((k, real, base[i], good)); ok &= good; continue
        allow = 2.15 * (mult[i] if not math.isinf(mult[i]) else 1.0) + 1.0   # VPA floors 3x (see p5_verify.py)
        good = (min(lo_e, hi_e) - allow - 0.5) <= real <= (max(lo_e, hi_e) + 1.0)
        rows.append((k, real, base[i], good)); ok &= good
    return ok, rows

# ---------------------------------------------------------------- install / run / cleanup
def cmd_install(a):
    if kubectl('top', 'nodes', check=False).returncode:
        print('metrics API not available -> installing metrics-server')
        kubectl('apply', '-f', 'https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml')
        kubectl('patch', 'deployment', 'metrics-server', '-n', 'kube-system', '--type=json', '-p',
                '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]')
        kubectl('rollout', 'status', 'deployment/metrics-server', '-n', 'kube-system', '--timeout=180s')
        for _ in range(30):
            if kubectl('top', 'nodes', check=False).returncode == 0: break
            time.sleep(5)
    else:
        print('metrics API already available')
    man = os.path.join(HERE, 'manifests')
    for f in ('vpa-v1-crd-gen.yaml', 'vpa-rbac.yaml', 'recommender-deployment.yaml'):
        kubectl('apply', '-f', os.path.join(man, f)); print('applied', f)
    kubectl('rollout', 'status', 'deployment/vpa-recommender', '-n', 'kube-system', '--timeout=240s')
    print(kubectl('get', 'pods', '-n', 'kube-system', '-l', 'app=vpa-recommender', '-o', 'wide').stdout)
    print('VPA 1.8.0 recommender is running (updater and admission controller are NOT installed: updateMode Off).')

def level_trace(seed, steps):
    """Piecewise-constant demand in cores, held for 2-5 steps, so sampling-time jitter hardly matters."""
    rnd = random.Random(seed); levels = [0.030, 0.045, 0.070, 0.100, 0.150, 0.210]; out = []
    while len(out) < steps: out += [rnd.choice(levels)] * rnd.randint(2, 5)
    return out[:steps]

def cmd_run(a):
    proto = load_proto(a.proto); steps = int(a.minutes * 60 / a.step)
    trace = level_trace(a.seed, steps); start = time.time() + 150
    cm = kubectl('create', 'configmap', CM, '--from-literal=replayer.py=' + proto.REPLAYER_CODE, '--dry-run=client', '-o', 'yaml').stdout
    kubectl('apply', '-f', '-', inp=cm)
    dep = {'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {'name': DEPLOY, 'namespace': NS},
           'spec': {'replicas': 1, 'selector': {'matchLabels': {'app': DEPLOY}},
                    'template': {'metadata': {'labels': {'app': DEPLOY}}, 'spec': {
                        'terminationGracePeriodSeconds': 0,
                        'containers': [{'name': 'replay', 'image': a.image, 'imagePullPolicy': 'IfNotPresent',
                                        'command': ['python3', '-u', '/app/replayer.py'],
                                        'env': [{'name': 'TRACE', 'value': ','.join(f'{v:.5f}' for v in trace)},
                                                {'name': 'STEP_S', 'value': str(a.step)},
                                                {'name': 'START_AT', 'value': f'{start:.3f}'}],
                                        'resources': {'requests': {'cpu': '100m', 'memory': '32Mi'},
                                                      'limits': {'memory': '64Mi'}},      # no CPU limit: usage = demand
                                        'volumeMounts': [{'name': 'code', 'mountPath': '/app'}]}],
                        'volumes': [{'name': 'code', 'configMap': {'name': CM}}]}}}}
    vpa = {'apiVersion': 'autoscaling.k8s.io/v1', 'kind': 'VerticalPodAutoscaler', 'metadata': {'name': VPA_NAME, 'namespace': NS},
           'spec': {'targetRef': {'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': DEPLOY},
                    'updatePolicy': {'updateMode': 'Off'}}}
    for o in (dep, vpa): kubectl('apply', '-f', '-', inp=json.dumps(o))
    out = a.out or time.strftime('live_run_%Y%m%d_%H%M%S'); os.makedirs(out, exist_ok=True)
    json.dump({'trace': trace, 'step_s': a.step, 'start_at': start, 'seed': a.seed}, open(os.path.join(out, 'meta.json'), 'w'))
    end = start + a.minutes * 60 + 240
    print(f'recording to {out}/polls.jsonl until {time.strftime("%H:%M:%S", time.localtime(end))} (about {(end - time.time()) / 60:.0f} min)')
    with open(os.path.join(out, 'polls.jsonl'), 'a') as fh:
        while time.time() < end:
            t0 = time.time()
            mt = kubectl('get', '--raw', f'/apis/metrics.k8s.io/v1beta1/namespaces/{NS}/pods?labelSelector=app%3D{DEPLOY}', check=False)
            cks = kjson('get', 'verticalpodautoscalercheckpoints.autoscaling.k8s.io', '-n', NS)
            vp = kjson('get', 'vpa', VPA_NAME, '-n', NS)
            ck = next((c for c in (cks or {}).get('items', []) if c['spec'].get('vpaObjectName') == VPA_NAME), None)
            fh.write(json.dumps({'t': t0, 'metrics': json.loads(mt.stdout) if mt.returncode == 0 and mt.stdout else None,
                                 'checkpoint': ck, 'vpa': vp}) + '\n'); fh.flush()
            time.sleep(max(0.0, a.poll - (time.time() - t0)))
    print('done. next:  python3 p5_live_check.py compare', out)

def cmd_cleanup(a):
    for k, n in (('vpa', VPA_NAME), ('deployment', DEPLOY), ('configmap', CM)):
        kubectl('delete', k, n, '-n', NS, '--ignore-not-found', check=False)
    if a.all:
        for f in ('recommender-deployment.yaml', 'vpa-rbac.yaml', 'vpa-v1-crd-gen.yaml'):
            kubectl('delete', '-f', os.path.join(HERE, 'manifests', f), '--ignore-not-found', check=False)
    print('cleaned up')

# ---------------------------------------------------------------- compare
def rec_of(vp):
    try: return vp['status']['recommendation']['containerRecommendations'][0]
    except (TypeError, KeyError, IndexError): return None

def settled_snapshots(polls):
    """One (checkpoint, recommendation) pair per recommender loop: the last poll whose state was identical to the previous poll
    (recommendation and checkpoint are written a few ms apart; this removes the rare poll that falls in between)."""
    key = lambda p: None if not p['checkpoint'] or not rec_of(p['vpa']) else (
        p['checkpoint']['status']['lastSampleStart'], p['checkpoint']['status']['totalSamplesCount'],
        json.dumps(rec_of(p['vpa']), sort_keys=True))
    snaps = {}
    for prev, cur in zip(polls, polls[1:]):
        k = key(cur)
        if k and k == key(prev): snaps[(k[0], k[1])] = cur
    return [snaps[k] for k in sorted(snaps, key=lambda x: x[1])]

def cmd_compare(a):
    proto = load_proto(a.proto)
    polls = [json.loads(l) for l in open(os.path.join(a.run_dir, 'polls.jsonl')) if l.strip()]
    snaps = settled_snapshots(polls)
    if not snaps: sys.exit('no usable (checkpoint, recommendation) pairs yet - was the recommender running for a few minutes?')
    print(f'{len(polls)} polls, {len(snaps)} recommender loops with a stable checkpoint + recommendation\n')
    print('== Check 1: P5 estimator on the real recommender histogram vs the real VPA recommendation (mCPU)')
    bad = 0
    for p in snaps:
        ok, rows = check1(proto, p['checkpoint'], rec_of(p['vpa'])); bad += (not ok)
        n = p['checkpoint']['status']['totalSamplesCount']
        print(f"{'ok  ' if ok else 'FAIL'} samples={n:3d}  " + '  '.join(
            f"{k[:5]}: VPA {r:9.1f} P5 {e:9.1f}" if not math.isinf(e) else f"{k[:5]}: VPA {r:9.1f} P5 inf" for k, r, e, g in rows))
    print(f'\nCheck 1 result: {"PASS" if not bad else "FAIL"} ({len(snaps) - bad}/{len(snaps)} loops reproduced)\n')

    print('== Check 2 (approximate): P5 fed with metrics-server usage at the recommender sample times vs real VPA')
    ms = []
    for p in polls:
        for it in ((p['metrics'] or {}).get('items') or []):
            c = it['containers'][0]['usage']['cpu']; ms.append((ts(it['timestamp']), q_mcpu(c) / 1000.0))
    if not ms: print('no metrics samples recorded'); sys.exit(1 if bad else 0)
    V = proto.VPAModel(1.0, 5.0, []); t0 = ts(snaps[0]['checkpoint']['status']['firstSampleStart']); within = 0; tot = 0; seen = set()
    marks = sorted({(ts(p['checkpoint']['status']['lastSampleStart']), p['checkpoint']['status']['totalSamplesCount']): p for p in snaps}.items())
    used = 0
    for (t_mark, n), p in marks:
        best = min(ms, key=lambda m: abs(m[0] - t_mark))
        if abs(best[0] - t_mark) > 20: continue
        V.add(best[1], t_mark - t0); used += 1
        est, _ = estimate(V); r = rec_of(p['vpa'])
        diffs = []
        for i, k in enumerate(('target', 'lowerBound', 'upperBound')):
            real = q_mcpu(cpu_of(r[k])); e = est[i]
            diffs.append(None if math.isinf(e) or real <= 0 else (e - real) / real)
        if diffs[0] is not None:
            tot += 1; within += abs(diffs[0]) <= 0.06
        print(f"loop {n:3d} usage {best[1] * 1000:6.1f}m  target VPA {q_mcpu(cpu_of(r['target'])):7.1f} P5 {est[0]:7.1f}  "
              + '  '.join('n/a' if d is None else f'{d * 100:+6.1f}%' for d in diffs))
    print(f'\nCheck 2: target within 6 % (about one bucket) in {within}/{tot} loops (informational; samples are not identical)')
    sys.exit(1 if bad else 0)

# ---------------------------------------------------------------- selftest (no cluster)
def cmd_selftest(a):
    proto = load_proto(a.proto); rnd = random.Random(3)
    cases = {'steps': [0.03] * 8 + [0.15] * 12 + [0.07] * 10 + [0.21] * 9,
             'walk': None, 'noisy': [max(0.0, 0.1 * (1 + rnd.gauss(0, .25))) for _ in range(120)]}
    v = 0.1; w = []
    for _ in range(300): v = max(0.002, v * math.exp(rnd.gauss(0, .06))); w.append(v)
    cases['walk'] = w
    tot = bad = 0
    for name, vals in cases.items():
        series = [[i * 60.0, x] for i, x in enumerate(vals)]
        r = subprocess.run([a.bin], input=json.dumps({'samples': series, 'queries': [s[0] for s in series], 'unit': 1000}),
                           capture_output=True, text=True, check=True)
        nb = 0
        for row in json.loads(r.stdout):
            if row['Upper'] >= 1e13 or row['Count'] < 1: continue
            base = 1_700_000_000
            ck = {'status': {'cpuHistogram': {'bucketWeights': {str(k): v for k, v in row['BucketWeights'].items()}, 'totalWeight': row['TotalWeight']},
                             'totalSamplesCount': row['Count'], 'firstSampleStart': iso(base + row['First']), 'lastSampleStart': iso(base + row['Last'])}}
            rec = {'target': {'cpu': f"{int(row['Target'])}m"}, 'lowerBound': {'cpu': f"{int(row['Lower'])}m"}, 'upperBound': {'cpu': f"{int(row['Upper'])}m"}}
            ok, rows = check1(proto, ck, rec); tot += 1; nb += (not ok)
            if not ok and nb <= 2: print('  mismatch', name, row['Count'], rows)
        bad += nb; print(f'{name:6s}: {len(series)} loops, {nb} not reproduced')
    print(f'selftest: {"PASS" if not bad else "FAIL"} ({tot - bad}/{tot} snapshots reproduced from real checkpoint data)')
    sys.exit(1 if bad else 0)

def main():
    ap = argparse.ArgumentParser(); sub = ap.add_subparsers(dest='cmd', required=True)
    dflt = os.path.join(HERE, '..', '..', 'k8s_prototype.py')   # verify_p5/live/ -> repo root
    for n, f in (('install', cmd_install), ('run', cmd_run), ('compare', cmd_compare), ('cleanup', cmd_cleanup), ('selftest', cmd_selftest)):
        p = sub.add_parser(n); p.set_defaults(f=f); p.add_argument('--proto', default=dflt)
        if n == 'run':
            p.add_argument('--minutes', type=float, default=35); p.add_argument('--step', type=float, default=30)
            p.add_argument('--poll', type=float, default=5); p.add_argument('--seed', type=int, default=5)
            p.add_argument('--image', default=os.environ.get('K8SVAL_IMAGE', 'python:3.12-slim')); p.add_argument('--out', default='')
        if n == 'compare': p.add_argument('run_dir')
        if n == 'cleanup': p.add_argument('--all', action='store_true')
        if n == 'selftest': p.add_argument('--bin', default=os.path.join(HERE, '..', 'vpacheck-linux-amd64'))
    a = ap.parse_args(); a.f(a)

if __name__ == '__main__':
    main()
