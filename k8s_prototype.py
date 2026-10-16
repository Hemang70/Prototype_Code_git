#!/usr/bin/env python3
"""
k8s_prototype.py - Section X-B: Kubernetes prototype, ONE FILE for Killercoda.

Runs the Paper-1 policies on a real Kubernetes node with in-place pod resize
and measures CPU from cgroup v2 every 5 s. Standard library only. Contains the
controller, the CPU replayer (installed as a ConfigMap) and an embedded smoke
trace. The workload traces come from ONE file, traces.json, written on Windows
by export_traces.py.

    python3 k8s_prototype.py --smoke                      # first session, ~10 min
    python3 k8s_prototype.py --datasets A --part 1/3 --run r1
    python3 k8s_prototype.py --keys A_03,A_S1 --run r1
    python3 k8s_prototype.py --offline --run sim          # same windows, simulator
                                                          # environment, seconds

HOW ONE BATCH WORKS
  One trace is started as one pod per policy (default P1, P2, P3, P6), at the
  same moment on the same node (paired comparison, identical background load).
  Every 5 s on a shared clock grid the controller
    1. reads each container's cpu.stat / cpu.max on the worker (one ssh call)
    2. finishes step k-1: usage = d(usage_usec)/dt, throttled = throttled
       periods / periods >= 0.20, applied limit = cpu.max
    3. passes the observation to the policy: usage, or - if throttled -
       limit x CENSOR_BUMP flagged as censored (resolved later)
    4. computes req/lim for step k and applies a change with an in-place
       resize (PATCH pods/<name>/resize), never a restart
  Results are appended after EVERY trace, so an expired session loses only the
  trace in progress; a rerun with the same --run skips finished traces.

POLICY LOGIC = class Controller in sim_paper1_all.py v1.5 (X-A). Check on
Windows:  python sim_paper1_all.py --check-smoke k8s_prototype.py
ENVIRONMENT differs by design (discuss in the paper):
  cluster : measured usage, CFS throttle ratio >= 0.20, limit enforced as
            cpu.max (systemd rounds it UP to 10 m), replayer overhead
  offline : --offline replays the same windows in the simulator environment
            (usage = min(demand, lim), throttled = demand > lim, exact limit;
            --enforce systemd adds the 10 m rounding)

Outputs: results/<run>/summary.csv, steps.csv, meta_*.json
         (offline: results/<run>_offline/). Tables: prototype_table.py.
"""
import argparse, csv, glob, json, math, os, statistics, subprocess, sys, time
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

POLICIES = ['P1', 'P2', 'P3', 'P6']            # default cluster set (fits Killercoda); add P5 with --policies
ALL_POLICIES = ['P1', 'P2', 'P3', 'P5', 'P6', 'P7', 'P8']   # P5: K8s VPA 1.8.0; P6: BA; P7, P8: ablations
API = 'http://127.0.0.1:8001'
NS = 'default'
IMAGE = os.environ.get('K8SVAL_IMAGE', 'python:3.12-slim')
LABEL = 'k8sval'
CENSOR_BUMP = 2.0    # stored value on a throttled step = applied limit x CENSOR_BUMP.
CENSOR_MODE = 'resolve'   # 'resolve' (v2) or 'keep' (v1: bumped values stay in the pool)

# --- policy logic: port of simulate() in sim_paper1_all.py ------------------
def up(x, d):
    """Round UP to d decimals (never allocate less than the policy computed).
    The -1e-7 absorbs float noise, e.g. 0.139 -> 139.00000000000003 m."""
    f = 10 ** d
    return math.ceil(x * f - 1e-7) / f

def safe_pct(data, p):
    if not data: return 0.0
    s = sorted(data); idx = (p / 100.0) * (len(s) - 1)
    lo = int(idx); hi = min(lo + 1, len(s) - 1)
    return s[lo] + (idx - lo) * (s[hi] - s[lo])


# --- P5: Kubernetes VPA recommender + updater (port of VPAController in sim_paper1_all.py v1.6)
# Constants verified line by line against kubernetes/autoscaler tag vertical-pod-autoscaler-1.8.0
# (commit 238579734e73), see P5_vs_VPA-1.8.0_diff.md.  Unlike the other policies, P5 is NOT scale invariant: its
# first histogram bucket (0.01 core), maximum (1000 cores) and minimum recommendation (25 m) are
# absolute.  The prototype traces are scaled down by t['scale'], so these three are multiplied by
# the same factor; then P5 on the scaled trace behaves as on the original trace.
VPA_GROWTH, VPA_HALF_LIFE_S, VPA_MARGIN = 0.05, 24 * 3600, 0.15
VPA_TARGET_PCT, VPA_LOWER_PCT, VPA_UPPER_PCT = 0.90, 0.50, 0.95
VPA_CONF_INT_S, VPA_SAMPLE_S, VPA_LIM_RATIO = 24 * 3600, 60, 1.15
VPA_MIN_SAMPLE_WEIGHT = 0.1          # constant weight in 1.8.0 (aggregate_container_state.go)

class VPAModel:
    def __init__(self, scale, step_s, pooled):
        r = 1.0 + VPA_GROWTH; self.r = r
        self.first = 0.01 * scale; self.min_cpu = 0.025 * scale; mx = 1000.0 * scale
        self.n = int(math.ceil(math.log(mx * (r - 1) / self.first + 1, r))) + 1
        self.w = [0.0] * self.n; self.total = 0.0
        self.first_t = self.last_t = None; self.count = 0
        self.per_min = max(int(round(VPA_SAMPLE_S / step_s)), 1)
        self.buf = []; self.tick = 0
        chunks = [pooled[i:i + self.per_min] for i in range(0, len(pooled), self.per_min)]
        chunks = [c for c in chunks if c]
        for j, c in enumerate(chunks):
            self.add(statistics.mean(c), -(len(chunks) - j) * VPA_SAMPLE_S)
    def start(self, i): return self.first * (self.r ** i - 1) / (self.r - 1)
    def find(self, v):
        if v < self.first: return 0
        i = int(math.floor(math.log(v * (self.r - 1) / self.first + 1, self.r)))
        return min(max(i, 0), self.n - 1)
    def add(self, v, t):
        wt = VPA_MIN_SAMPLE_WEIGHT * 2.0 ** (t / VPA_HALF_LIFE_S)
        self.w[self.find(max(v, 0.0))] += wt; self.total += wt; self.count += 1
        self.first_t = t if self.first_t is None else min(self.first_t, t)
        self.last_t = t if self.last_t is None else max(self.last_t, t)
    def pct(self, q):
        if self.total <= 0: return 0.0
        thr, acc = q * self.total, 0.0
        for i, x in enumerate(self.w):
            acc += x
            if acc >= thr: break
        return self.start(i + 1) if i < self.n - 1 else self.start(i)
    def target(self): return max(self.pct(VPA_TARGET_PCT) * (1 + VPA_MARGIN), self.min_cpu)
    def bounds(self):
        c = min((self.last_t - self.first_t) / VPA_CONF_INT_S,
                self.count / (VPA_CONF_INT_S / 60.0)) if self.count else 0.0
        lo = self.pct(VPA_LOWER_PCT) * (1 + VPA_MARGIN); hi = self.pct(VPA_UPPER_PCT) * (1 + VPA_MARGIN)
        lo = lo * (1 + 0.001 / c) ** -2.0 if c > 0 else 0.0
        hi = hi * (1 + 1.0 / c) if c > 0 else float('inf')
        return max(lo, self.min_cpu), max(hi, self.min_cpu)
    def observe(self, usage):
        """delivered CPU only: real VPA cannot see throttled demand"""
        self.buf.append(usage)
        if len(self.buf) == self.per_min:
            self.tick += 1
            self.add(statistics.mean(self.buf), self.tick * VPA_SAMPLE_S); self.buf = []

class Policy:
    """State of one policy on one pod (observable usage, millicore output)."""
    def __init__(self, name, t, bump=None, mode=None):
        self.p = name
        self.CE = t['control_every']; self.MIN_OBS = t['min_obs']
        self.TH = t['change_thresh']
        self.BUMP = CENSOR_BUMP if bump is None else bump
        self.MODE = CENSOR_MODE if mode is None else mode
        self.M1 = t['p1_margin']; self.DEF_B = t['default_b']
        self.CAP = t['pool_cap']; self.W = t['w_warm']
        self.obs = list(t['pooled_seed'])
        self.cens = [None] * len(self.obs)   # None = true value, else censoring lower bound
        self.last_thr = False
        self.resizes = 0; self.resizes_meas = 0; self.resolved = 0
        self.last_out = None
        obs = self.obs
        self.vpa = None
        if name == 'P5':
            self.vpa = VPAModel(t.get('scale', 1.0), t['step_s'], list(self.obs))
            req = self.vpa.target() if self.vpa.count else t['cold_request']
            self.cur_req = max(req, 0.001); self.cur_lim = self.cur_req * VPA_LIM_RATIO
            return
        if obs:
            p90 = safe_pct(obs, 90.0); mu = statistics.mean(obs)
            sd = statistics.pstdev(obs) if len(obs) > 1 else 0.0
            b = self.burst(obs)
            req = safe_pct(obs, 98 - 28 * b); lim = max(obs) * (1.15 - 0.10 * b)
            if name == 'P2':   req = p90; lim = p90 * 1.15
            elif name == 'P3': req = lim = mu + 3 * sd
            elif name == 'P1': req = lim = max(obs) * self.M1
            elif name == 'P7': lim = max(obs) * 1.15          # fixed headroom
            elif name == 'P8': req = p90                      # fixed percentile
        else:
            cold = t['cold_request']
            req = cold; lim = cold * 1.15
            if name == 'P1': lim = req
        req = max(req, 0.001); lim = max(lim, req)
        self.cur_req, self.cur_lim = req, lim

    def burst(self, obs):
        if not obs or max(obs) == 0: return self.DEF_B
        mx = max(obs); mn = statistics.mean(obs)
        return min(max((mx - mn) / mx, 0.0), 1.0)

    def decide(self, k):
        """Allocation (cores, millicore-rounded) for step k; uses obs up to k-1."""
        obs, p, TH = self.obs, self.p, self.TH
        if p == 'P5':
            V = self.vpa
            if k > 0 and k % V.per_min == 0 and V.count:     # recommender + updater every 1 min
                lo, hi = V.bounds()
                if self.cur_req < lo or self.cur_req > hi:   # updater acts only outside [lower, upper]
                    self.cur_req = V.target(); self.cur_lim = self.cur_req * VPA_LIM_RATIO
            req, lim = self.cur_req, max(self.cur_lim, self.cur_req)
            req = max(up(req, 3), 0.001); lim = max(up(lim, 3), req)
            if self.last_out is not None and (req, lim) != self.last_out:
                self.resizes += 1
                if k >= self.W: self.resizes_meas += 1
            self.last_out = (req, lim)
            return req, lim
        tick = (k > 0) and (k % self.CE == 0)
        trig = self.last_thr                     # re-evaluation trigger: step k-1 throttled
        cr, cl = self.cur_req, self.cur_lim
        if p == 'P1':
            if tick and obs:
                new = up(max(obs) * self.M1, 4)
                if abs(new - cr) / max(cr, 0.001) > TH or trig:
                    self.cur_req = self.cur_lim = new
            req = lim = self.cur_req
        elif p == 'P2':
            if tick and len(obs) >= 2:
                r = safe_pct(obs, 90.0); nl = up(r * 1.15, 4)
                if abs(r - cr) / max(cr, 0.001) > TH or trig:
                    self.cur_req, self.cur_lim = r, nl
            req, lim = self.cur_req, self.cur_lim
        elif p == 'P3':
            if tick and len(obs) >= 2:
                mu = statistics.mean(obs)
                sd = statistics.pstdev(obs) if len(obs) > 1 else 0.0
                val = up(mu + 3 * sd, 4)
                if abs(val - cr) / max(cr, 0.001) > TH or trig:
                    self.cur_req = self.cur_lim = val
            req = lim = self.cur_req
        elif p in ('P6', 'P7', 'P8'):
            # P7 = adaptive percentile, fixed headroom 1.15; P8 = fixed P90, adaptive headroom
            if tick and len(obs) >= self.MIN_OBS:
                b = self.burst(obs)
                pct = 90.0 if p == 'P8' else 98.0 - 28.0 * b
                lh = 1.15 if p == 'P7' else 1.15 - 0.10 * b
                nr = up(safe_pct(obs, pct), 4)
                nl = up(max(obs) * lh, 4); nl = max(nl, nr)
                if (abs(nr - cr) / max(cr, 0.001) > TH
                        or abs(nl - cl) / max(cl, 0.001) > TH or trig):
                    self.cur_req, self.cur_lim = nr, nl
            req, lim = self.cur_req, max(self.cur_lim, self.cur_req)
        else:
            raise ValueError(p)
        req = max(req, 0.001); lim = max(lim, req)
        req = max(up(req, 3), 0.001); lim = max(up(lim, 3), req)   # millicores, rounded UP
        # a resize is counted only if the millicore allocation really changed
        if self.last_out is not None and (req, lim) != self.last_out:
            self.resizes += 1
            if k >= self.W: self.resizes_meas += 1
        self.last_out = (req, lim)
        return req, lim

    def observe(self, usage, lim, throttled):
        """lim = limit in force during the step (applied cpu.max if known).
        Returns (stored value, censored flag)."""
        if self.p == 'P5':                  # no censoring rule: delivered CPU only
            self.vpa.observe(usage); self.obs.append(usage)
            if len(self.obs) > self.CAP: self.obs = self.obs[-self.CAP:]
            self.last_thr = throttled
            return usage, False
        if throttled:
            seen = lim * self.BUMP
            self.obs.append(seen); self.cens.append(lim)
        else:
            seen = usage
            self.obs.append(seen); self.cens.append(None)
            if self.MODE == 'resolve':
                # an unthrottled step at a higher limit reveals the demand level:
                # older censored samples (true demand >= their bound) are replaced
                for i, c in enumerate(self.cens):
                    if c is not None and lim > c + 1e-9:
                        self.obs[i] = max(c, usage); self.cens[i] = None
                        self.resolved += 1
        if len(self.obs) > self.CAP:
            self.obs = self.obs[-self.CAP:]; self.cens = self.cens[-self.CAP:]
        self.last_thr = throttled
        return seen, throttled

def systemd_quota(lim):
    """cpu.max actually applied with the systemd cgroup driver (rounded up to 10m)."""
    return math.ceil(round(lim * 1000) / 10 - 1e-9) / 100

# --- Kubernetes API through `kubectl proxy` (no Python packages needed) -----
def api(method, path, body=None, ctype='application/json', ok404=False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method,
                                 headers={'Content-Type': ctype})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            txt = r.read().decode()
            return json.loads(txt) if txt else {}
    except urllib.error.HTTPError as e:
        if e.code == 404 and ok404: return None
        raise RuntimeError(f'{method} {path} -> {e.code}: {e.read().decode()[:300]}')

def ensure_proxy():
    try:
        api('GET', '/version'); return None
    except Exception:
        pass
    proc = subprocess.Popen(['kubectl', 'proxy', '--port=8001'],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        time.sleep(0.2)
        try:
            api('GET', '/version'); return proc
        except Exception:
            pass
    sys.exit('[ERROR] kubectl proxy did not start')

def mcpu(x): return f'{int(round(x * 1000))}m'

def pod_manifest(name, key, pol, trace, step_s, start_at, req, lim, node):
    return {'apiVersion': 'v1', 'kind': 'Pod',
            'metadata': {'name': name, 'labels': {'app': LABEL, 'key': key.lower().replace('_', '-'),
                                                  'policy': pol.lower()}},
            'spec': {'nodeName': node, 'restartPolicy': 'Never',
                     'terminationGracePeriodSeconds': 0,
                     'containers': [{
                         'name': 'replay', 'image': IMAGE, 'imagePullPolicy': 'IfNotPresent',
                         'command': ['python3', '-u', '/app/replayer.py'],
                         'env': [{'name': 'TRACE', 'value': ','.join(f'{v:.5f}' for v in trace)},
                                 {'name': 'STEP_S', 'value': str(step_s)},
                                 {'name': 'START_AT', 'value': f'{start_at:.3f}'}],
                         # memory req != lim keeps the pod Burstable for every policy,
                         # so no resize can change its QoS class (which K8s forbids)
                         'resources': {'requests': {'cpu': mcpu(req), 'memory': '32Mi'},
                                       'limits': {'cpu': mcpu(lim), 'memory': '64Mi'}},
                         'resizePolicy': [{'resourceName': 'cpu', 'restartPolicy': 'NotRequired'}],
                         'volumeMounts': [{'name': 'code', 'mountPath': '/app'}]}],
                     'volumes': [{'name': 'code', 'configMap': {'name': 'k8sval-replayer'}}]}}

def resize(name, req, lim):
    body = {'spec': {'containers': [{'name': 'replay', 'resources': {
        'requests': {'cpu': mcpu(req)}, 'limits': {'cpu': mcpu(lim)}}}]}}
    api('PATCH', f'/api/v1/namespaces/{NS}/pods/{name}/resize', body,
        ctype='application/strategic-merge-patch+json')

def delete_all(wait=True):
    api('DELETE', f'/api/v1/namespaces/{NS}/pods?labelSelector=app%3D{LABEL}',
        {'kind': 'DeleteOptions', 'apiVersion': 'v1', 'gracePeriodSeconds': 0})
    if not wait: return
    for _ in range(120):
        items = api('GET', f'/api/v1/namespaces/{NS}/pods?labelSelector=app%3D{LABEL}')['items']
        if not items: return
        time.sleep(1)

def setup(node, here):
    code = REPLAYER_CODE
    api('DELETE', f'/api/v1/namespaces/{NS}/configmaps/k8sval-replayer', ok404=True)
    api('POST', f'/api/v1/namespaces/{NS}/configmaps',
        {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'k8sval-replayer'},
         'data': {'replayer.py': code}})
    delete_all()
    print(f'  pre-pulling {IMAGE} on {node} ...', flush=True)
    api('DELETE', f'/api/v1/namespaces/{NS}/pods/k8sval-prepull', ok404=True)
    time.sleep(2)
    api('POST', f'/api/v1/namespaces/{NS}/pods', {
        'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'k8sval-prepull'},
        'spec': {'nodeName': node, 'restartPolicy': 'Never', 'terminationGracePeriodSeconds': 0,
                 'containers': [{'name': 'p', 'image': IMAGE, 'command': ['true']}]}})
    for _ in range(300):
        ph = api('GET', f'/api/v1/namespaces/{NS}/pods/k8sval-prepull')['status'].get('phase')
        if ph in ('Succeeded', 'Failed'): break
        time.sleep(1)
    api('DELETE', f'/api/v1/namespaces/{NS}/pods/k8sval-prepull', ok404=True)
    print('  image ready', flush=True)

# --- cgroup samplers ----------------------------------------------------------
def parse_cgroup(text):
    """Parse 'cpu.stat' lines + one 'cpu.max' line into a dict."""
    d = {}
    for line in text.splitlines():
        f = line.split()
        if len(f) == 2 and f[0] in ('usage_usec', 'nr_periods', 'nr_throttled', 'throttled_usec'):
            d[f[0]] = int(f[1])
        elif len(f) == 2 and (f[0] == 'max' or f[0].isdigit()) and f[1].isdigit():
            d['applied_lim'] = (None if f[0] == 'max' else int(f[0]) / int(f[1]))
    return d if 'usage_usec' in d else None

class SshSampler:
    """Reads all containers' cgroup files on the worker in ONE ssh call per step."""
    OPTS = ['-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=no', '-o', 'LogLevel=ERROR',
            '-o', 'ControlMaster=auto', '-o', 'ControlPath=/tmp/k8sval-ssh-%h',
            '-o', 'ControlPersist=1200']
    def __init__(self, node): self.node = node
    def check(self):
        # remove a stale multiplexing socket left by an earlier run (it can hang ssh)
        for f in glob.glob('/tmp/k8sval-ssh-*'):
            try: os.remove(f)
            except OSError: pass
        try:
            r = subprocess.run(['ssh', *self.OPTS, '-o', 'ConnectTimeout=10', self.node,
                                'cat /sys/fs/cgroup/cgroup.controllers'],
                               capture_output=True, text=True, timeout=30)
            return r.returncode == 0
        except subprocess.TimeoutExpired:
            return False
    def read(self, jobs):
        parts = []
        for j in jobs:
            parts.append(f'echo "@@{j["name"]}"; cat {j["cg"]}/cpu.stat {j["cg"]}/cpu.max 2>/dev/null')
        try:
            r = subprocess.run(['ssh', *self.OPTS, self.node, '; '.join(parts)],
                               capture_output=True, text=True, timeout=10)
        except subprocess.TimeoutExpired:
            return {}                     # counted as missed samples for this step
        out, cur, buf = {}, None, []
        for line in r.stdout.splitlines() + ['@@__end']:
            if line.startswith('@@'):
                if cur: out[cur] = parse_cgroup('\n'.join(buf))
                cur, buf = line[2:], []
            else:
                buf.append(line)
        return out

class ExecSampler:
    """Fallback: `kubectl exec` into each container (slower, parallel threads)."""
    def check(self): return True
    def _one(self, name):
        r = subprocess.run(['kubectl', 'exec', name, '-c', 'replay', '--', 'cat',
                            '/sys/fs/cgroup/cpu.stat', '/sys/fs/cgroup/cpu.max'],
                           capture_output=True, text=True, timeout=10)
        return name, parse_cgroup(r.stdout)
    def read(self, jobs):
        with ThreadPoolExecutor(len(jobs)) as ex:
            return dict(ex.map(lambda j: self._one(j['name']), jobs))

# --- per-step rows and per-policy summary (shared by cluster and offline) ----
def make_row(run_id, t, pol, k, demand, usage, seen, req, lim, applied_lim,
             thr_ratio, throttled, censored, resized, dt):
    return dict(run=run_id, key=t['key'], dataset=t['dataset'], policy=pol,
                step=k, warmup=int(k < t['w_warm']), demand=round(demand, 6),
                usage=round(usage, 6), observed=round(seen, 6), req=req, lim=lim,
                applied_lim=('' if applied_lim is None else round(applied_lim, 4)),
                thr_ratio=round(thr_ratio, 4), throttled=int(throttled),
                censored=int(censored), resized=resized,
                over_req=round(max(req - demand, 0.0), 6),
                under_req=round(max(demand - req, 0.0), 6), dt=round(dt, 3))

def summarize(run_id, mode, t, pol, rows, P, patches, restarts, missed, base):
    m = [r for r in rows if not r['warmup']]
    n = max(len(m), 1)
    avg = lambda f: sum(r[f] for r in m) / n
    alim = lambda r: r['applied_lim'] if r['applied_lim'] != '' else r['lim']
    ms = lambda x: round(x * 1000, 2)
    return dict(
        run=run_id, mode=mode, key=t['key'], dataset=t['dataset'],
        set=t.get('set', ''), group=t.get('group', ''), policy=pol,
        censor_bump=P.BUMP, censor_mode=P.MODE, censor_base=base,
        avg_req_mcpu=ms(avg('req')), avg_lim_mcpu=ms(avg('lim')),
        avg_applied_lim_mcpu=ms(sum(alim(r) for r in m) / n),
        avg_actual_mcpu=ms(avg('demand')), avg_eff_mcpu=ms(avg('usage')),
        over_req_mcpu=ms(avg('over_req')), under_req_mcpu=ms(avg('under_req')),
        unserved_mcpu=ms(sum(max(r['demand'] - r['usage'], 0.0) for r in m) / n),
        lim_headroom_mcpu=ms(sum(alim(r) - r['usage'] for r in m) / n),
        throttle_pct=round(sum(r['throttled'] for r in m) / n * 100, 2),
        mean_thr_ratio=round(avg('thr_ratio'), 4),
        resizes=P.resizes_meas, k8s_patches=patches, restarts=restarts,
        missed_samples=missed, censored_steps=sum(r['censored'] for r in m),
        resolved=P.resolved, meas_steps=len(m))

# --- offline: same Policy code on an idealised node --------------------------
def run_offline(t, policies, args, run_id):
    S = t['steps']; trace = t['cpu_trace'][:S]
    summ, steps = [], []
    for pol in policies:
        P = Policy(pol, t, args.censor_bump, args.censor_mode)
        rows, patches, applied = [], 0, None
        for k in range(S):
            before = P.resizes_meas
            req, lim = P.decide(k)
            want = (mcpu(req), mcpu(lim))
            if applied is not None and want != applied: patches += 1
            applied = want
            al = systemd_quota(lim) if getattr(args, 'enforce', 'systemd') == 'systemd' else lim
            d = trace[k]
            thr = d > al + 1e-9
            usage = min(d, al)
            seen, cens = P.observe(usage, al if args.censor_base == 'applied' else lim, thr)
            rows.append(make_row(run_id, t, pol, k, d, usage, seen, req, lim, al,
                                 1.0 if thr else 0.0, thr, cens,
                                 int(P.resizes_meas > before), t['step_s']))
        summ.append(summarize(run_id, 'offline', t, pol, rows, P, patches, 0, 0,
                              args.censor_base))
        steps += rows
    return summ, steps

# --- cluster: one batch = one trace x all policies ---------------------------
def run_batch(t, policies, args, sampler, run_id):
    S, STEP = t['steps'], t['step_s']
    trace = t['cpu_trace'][:S]
    jobs = []
    for pol in policies:
        name = f"kv-{t['key'].lower().replace('_', '-')}-{pol.lower()}"
        jobs.append(dict(name=name, pol=pol, P=Policy(pol, t, args.censor_bump, args.censor_mode),
                         rows=[], applied=None, patches=0, missed=0, restarts=0))
    for j in jobs:
        j['req'], j['lim'] = j['P'].decide(0)

    start_at = time.time() + args.lead
    for j in jobs:
        api('POST', f'/api/v1/namespaces/{NS}/pods',
            pod_manifest(j['name'], t['key'], j['pol'], trace, STEP, start_at,
                         j['req'], j['lim'], args.node))
        j['applied'] = (mcpu(j['req']), mcpu(j['lim']))
    # wait until all containers run, then locate their cgroups
    while True:
        pending = []
        for j in jobs:
            if 'cg' in j: continue
            st = api('GET', f'/api/v1/namespaces/{NS}/pods/{j["name"]}')
            cs = (st['status'].get('containerStatuses') or [{}])[0]
            if st['status'].get('phase') == 'Failed':
                raise RuntimeError(f'{j["name"]} failed: {st["status"]}')
            if 'running' in cs.get('state', {}) and cs.get('containerID'):
                uid = st['metadata']['uid'].replace('-', '_')
                cid = cs['containerID'].split('://', 1)[1]
                qos = st['status'].get('qosClass', 'Burstable').lower()
                base = '/sys/fs/cgroup/kubepods.slice'
                if qos == 'guaranteed':
                    j['cg'] = f'{base}/kubepods-pod{uid}.slice/cri-containerd-{cid}.scope'
                else:
                    j['cg'] = (f'{base}/kubepods-{qos}.slice/kubepods-{qos}-pod{uid}.slice/'
                               f'cri-containerd-{cid}.scope')
            else:
                pending.append(j['name'])
        if not pending: break
        if time.time() > start_at - 2:
            for name in pending:          # explain WHY the pods did not start
                try:
                    st = api('GET', f'/api/v1/namespaces/{NS}/pods/{name}')['status']
                    cs = (st.get('containerStatuses') or [{}])[0].get('state', {})
                    cond = [f"{c['type']}={c['status']}:{c.get('reason', '')}"
                            for c in st.get('conditions', []) if c['status'] != 'True']
                    print(f'    {name}: phase={st.get("phase")} state={cs} '
                          f'{st.get("reason", "")} {st.get("message", "")} {cond}', flush=True)
                except Exception as e:
                    print(f'    {name}: status unavailable ({e})', flush=True)
            subprocess.run('kubectl get events --sort-by=.lastTimestamp | tail -8', shell=True)
            raise RuntimeError(f'pods not running before start: {pending} '
                               f'(increase --lead)')
        time.sleep(1)

    prev = {}; t_prev = None
    for k in range(S + 1):
        target = start_at + k * STEP + 0.05
        now = time.time()
        if now < target: time.sleep(target - now)
        t_read = time.time()
        snap = sampler.read(jobs)
        for j in jobs:
            P = j['P']
            # -- finish step k-1 ------------------------------------------
            if k > 0:
                demand = trace[k - 1]
                a, b = prev.get(j['name']), snap.get(j['name'])
                dt = t_read - t_prev
                if a and b and dt > 0:
                    usage = (b['usage_usec'] - a['usage_usec']) / 1e6 / dt
                    dp = b['nr_periods'] - a['nr_periods']
                    thr_ratio = (b['nr_throttled'] - a['nr_throttled']) / dp if dp > 0 else 0.0
                    applied_lim = b.get('applied_lim')
                else:
                    j['missed'] += 1
                    usage = P.obs[-1] if P.obs else 0.0; thr_ratio = 0.0; applied_lim = None
                throttled = thr_ratio >= args.thr_ratio
                # censoring bound = limit really enforced by the kernel (cpu.max)
                bound = (applied_lim if (applied_lim and args.censor_base == 'applied')
                         else j['lim'])
                seen, cens = P.observe(usage, bound, throttled)
                j['rows'].append(make_row(run_id, t, j['pol'], k - 1, demand, usage, seen,
                                          j['req'], j['lim'], applied_lim, thr_ratio,
                                          throttled, cens, j.pop('_resized', 0), dt))
            # -- allocation for step k ------------------------------------
            if k < S:
                before = P.resizes_meas
                req, lim = P.decide(k) if k > 0 else (j['req'], j['lim'])
                j['_resized'] = int(P.resizes_meas > before)
                j['req'], j['lim'] = req, lim
                want = (mcpu(req), mcpu(lim))
                if want != j['applied']:
                    try:
                        resize(j['name'], req, lim); j['applied'] = want; j['patches'] += 1
                    except Exception as e:
                        print(f'    [WARN] resize {j["name"]} failed: {e}', flush=True)
        prev, t_prev = snap, t_read
        if k % 24 == 0 or k == S:
            print(f'    step {k:>3}/{S}  ' + '  '.join(
                f'{j["pol"]}:{mcpu(j["req"])}/{mcpu(j["lim"])}' for j in jobs), flush=True)

    for j in jobs:               # container restarts (in-place resize must cause none)
        st = api('GET', f'/api/v1/namespaces/{NS}/pods/{j["name"]}')
        j['restarts'] = sum(c.get('restartCount', 0)
                            for c in st['status'].get('containerStatuses') or [])
    delete_all()
    summary = [summarize(run_id, 'cluster', t, j['pol'], j['rows'], j['P'],
                         j['patches'], j['restarts'], j['missed'], args.censor_base)
               for j in jobs]
    return summary, [r for j in jobs for r in j['rows']]

def append_csv(path, rows):
    if not rows: return
    fields = list(rows[0].keys())
    if os.path.isfile(path):
        with open(path, newline='', encoding='utf-8') as f:
            head = next(csv.reader(f), None)
        if head != fields:       # e.g. a v1 file: never mix column layouts
            old = f'{path}.old_{int(time.time())}'
            os.rename(path, old)
            print(f'  [INFO] {path} had a different header - moved to {old}', flush=True)
    new = not os.path.isfile(path)
    with open(path, 'a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator='\n')
        if new: w.writeheader()
        w.writerows(rows)

def print_summary(summ):
    for s in summ:
        print(f'    {s["policy"]}: req {s["avg_req_mcpu"]:.0f}m  lim {s["avg_lim_mcpu"]:.0f}m'
              f'  used {s["avg_eff_mcpu"]:.0f}m  over {s["over_req_mcpu"]:.0f}m'
              f'  under {s["under_req_mcpu"]:.0f}m  thr {s["throttle_pct"]:.1f}%'
              f'  resizes {s["resizes"]}  patches {s["k8s_patches"]}'
              f'  restarts {s["restarts"]}  missed {s["missed_samples"]}', flush=True)

def load_traces(path):
    """traces.json from export_traces.py ({"traces": [...]}), a list, or a folder
    of per-pod JSON files (older export)."""
    if os.path.isdir(path):
        return [json.load(open(f, encoding='utf-8'))
                for f in sorted(glob.glob(os.path.join(path, '*.json')))], None
    if not os.path.isfile(path):
        sys.exit(f'[ERROR] {path} not found - run export_traces.py on Windows '
                 f'and commit traces.json, or use --smoke')
    doc = json.load(open(path, encoding='utf-8'))
    if isinstance(doc, list): return doc, None
    return doc['traces'], doc

def file_md5(path):
    import hashlib
    if not os.path.isfile(path): return ''
    return hashlib.md5(open(path, 'rb').read()).hexdigest()[:12]

def done_keys(out, policies):
    """Traces already finished in this run (all policies present)."""
    f = os.path.join(out, 'summary.csv')
    if not os.path.isfile(f): return set()
    have = {}
    with open(f, newline='', encoding='utf-8') as fh:
        for r in csv.DictReader(fh):
            have.setdefault(r.get('key'), set()).add(r.get('policy'))
    return {k for k, p in have.items() if set(policies) <= p}

def git_push(msg):
    """Commit and push results/ (used with --push so an expiring session keeps
    every finished trace). Never stops the run on a git error."""
    cmds = [['git', 'add', 'results'],
            ['git', '-c', 'user.name=k8s', '-c', 'user.email=k8s@localhost',
             'commit', '-qm', msg],
            ['git', 'pull', '--rebase', '-q'],
            ['git', 'push', '-q']]
    for c in cmds:
        r = subprocess.run(c, capture_output=True, text=True)
        if r.returncode != 0 and c[1] != 'commit':
            print(f'    [WARN] {" ".join(c[:2])} failed: {(r.stderr or r.stdout).strip()[:200]}',
                  flush=True)
            return False
    print('    results pushed to git', flush=True)
    return True

def main():
    ap = argparse.ArgumentParser(description='Kubernetes prototype for Paper 1 policies (X-B)')
    ap.add_argument('--traces', default='traces.json', help='traces.json (or a folder of JSONs)')
    ap.add_argument('--smoke', action='store_true',
                    help='run the embedded A_S1 trace (no traces.json needed)')
    ap.add_argument('--embedded', default='', help='embedded keys, e.g. A_SYN_DRIFT,A_SYN_BURST')
    ap.add_argument('--datasets', default='A,B', help='e.g. A or A,B')
    ap.add_argument('--keys', default='', help='explicit keys, e.g. A_00,A_S1 (overrides --part)')
    ap.add_argument('--part', default='1/1', help='i/n: run the i-th of n equal parts')
    ap.add_argument('--policies', default=','.join(POLICIES),
                    help=f'default {",".join(POLICIES)}; P5 (K8s VPA), P7, P8 also available '
                         f'(export traces with --concurrent 7 first)')
    ap.add_argument('--run', default=None, help='run label: r1, r2, r3 (default smoke/r1)')
    ap.add_argument('--out', default='results')
    ap.add_argument('--no-resume', action='store_true', help='rerun traces already in this run')
    ap.add_argument('--push', action='store_true',
                    help='git commit + push results/ after every trace (run_session.sh uses it)')
    ap.add_argument('--image', default=None, help='container image (default python:3.12-slim)')
    ap.add_argument('--node', default='node01')
    ap.add_argument('--sampler', choices=['ssh', 'exec'], default='ssh')
    ap.add_argument('--thr-ratio', type=float, default=0.20,
                    help='step counts as throttled if >= this fraction of CFS periods throttled')
    ap.add_argument('--lead', type=float, default=60.0, help='seconds between pod creation and step 0')
    ap.add_argument('--censor-bump', type=float, default=CENSOR_BUMP,
                    help='stored value on a throttled step = limit x this')
    ap.add_argument('--censor-mode', choices=['resolve', 'keep'], default=CENSOR_MODE)
    ap.add_argument('--censor-base', choices=['applied', 'requested'], default='applied',
                    help='censoring bound: applied cpu.max (default) or requested limit')
    ap.add_argument('--offline', action='store_true',
                    help='replay the windows in the simulator environment (no cluster)')
    ap.add_argument('--enforce', choices=['natural', 'systemd'], default='natural',
                    help='offline only: exact limit (= simulator) or 10 m rounding (= cluster)')
    args = ap.parse_args()
    global IMAGE
    if args.image: IMAGE = args.image
    here = os.path.dirname(os.path.abspath(__file__))
    policies = args.policies.split(',')
    bad = [p for p in policies if p not in ALL_POLICIES]
    if bad: sys.exit(f'[ERROR] unknown policies {bad}; choose from {ALL_POLICIES}')

    # ---- select traces
    if args.smoke or args.embedded:
        keys = ['A_S1'] if args.smoke and not args.embedded else args.embedded.split(',')
        emb = {t['key']: t for t in EMBEDDED_TRACES}
        traces = [emb[k] for k in keys if k in emb]
        run = args.run or 'smoke'; src = 'embedded'
    else:
        all_t, doc = load_traces(args.traces)
        ds = set(args.datasets.split(','))
        pool = sorted((t for t in all_t if t['dataset'] in ds), key=lambda t: t['key'])
        if args.keys:
            want = args.keys.split(',')
            traces = [t for t in pool if t['key'] in want]
            missing = [k for k in want if k not in {t['key'] for t in traces}]
            if missing: print(f'  [WARN] keys not in {args.traces}: {missing}')
        else:
            i, n = map(int, args.part.split('/'))
            traces = [t for idx, t in enumerate(pool) if idx * n // len(pool) == i - 1] if pool else []
        run = args.run or 'r1'; src = f'{args.traces} (md5 {file_md5(args.traces)})'
        if doc and doc.get('settings'):
            st = doc['settings']
            print(f'traces.json: {len(all_t)} traces, exported {doc.get("created", "?")}, '
                  f'settings {st}', flush=True)
    if not traces: sys.exit('[ERROR] no traces selected - check --traces/--datasets/--keys/--part')

    out = os.path.join(args.out, run + ('_offline' if args.offline else ''))
    os.makedirs(out, exist_ok=True)
    if not args.no_resume:
        done = done_keys(out, policies)
        skipped = [t['key'] for t in traces if t['key'] in done]
        traces = [t for t in traces if t['key'] not in done]
        if skipped: print(f'  resume: {", ".join(skipped)} already finished in {out} - skipped',
                          flush=True)
        if not traces: print('Nothing left to run.'); return
    cfg = (f'censor bump {args.censor_bump} ({args.censor_mode}, {args.censor_base} limit)')

    if args.offline:
        print(f'OFFLINE ({args.enforce} enforcement)  traces: {", ".join(t["key"] for t in traces)}'
              f'  | policies: {",".join(policies)}  | {cfg}  | output: {out}', flush=True)
        for t in traces:
            summ, steps = run_offline(t, policies, args, run)
            for s in summ: s['mode'] = f'offline-{args.enforce}'
            append_csv(os.path.join(out, 'summary.csv'), summ)
            append_csv(os.path.join(out, 'steps.csv'), steps)
            print(f'\n  {t["key"]}  peak={max(t["cpu_trace"]) * 1000:.0f}m', flush=True)
            print_summary(summ)
        print(f'\nDone. Results in {out}/summary.csv and steps.csv')
        return

    nodes = subprocess.run(['kubectl', 'get', 'node', args.node, '--no-headers'],
                           capture_output=True, text=True).stdout.split()
    print(f'node {args.node}: {nodes[1] if len(nodes) > 1 else "NOT FOUND"}', flush=True)
    if len(nodes) < 2 or nodes[1] != 'Ready':
        sys.exit('[ERROR] worker node is not Ready - open a new Killercoda playground')
    per = traces[0]['steps'] * traces[0]['step_s'] / 60 + args.lead / 60 + 0.5
    print(f'Traces: {", ".join(t["key"] for t in traces)}  | policies: {",".join(policies)}'
          f'  | {cfg}  | ~{per * len(traces):.0f} min  | output: {out}', flush=True)

    proc = ensure_proxy()
    sampler = SshSampler(args.node) if args.sampler == 'ssh' else ExecSampler()
    if not sampler.check():
        print('  [WARN] ssh to node failed - falling back to kubectl exec sampler', flush=True)
        sampler = ExecSampler()
    v = api('GET', '/version')
    try:
        alloc = api('GET', f'/api/v1/nodes/{args.node}')['status']['allocatable'].get('cpu')
    except Exception:
        alloc = '?'
    json.dump(dict(version='k8s_prototype.py', k8s=v.get('gitVersion'), node=args.node,
                   node_allocatable_cpu=alloc, sampler=type(sampler).__name__,
                   thr_ratio=args.thr_ratio, lead_s=args.lead, policies=policies,
                   censor_bump=args.censor_bump, censor_mode=args.censor_mode,
                   censor_base=args.censor_base, traces_source=src,
                   keys=[t['key'] for t in traces],
                   started=time.strftime('%Y-%m-%d %H:%M:%S')),
              open(os.path.join(out, f'meta_{int(time.time())}.json'), 'w'), indent=1)
    print(f'  node allocatable cpu: {alloc}', flush=True)
    setup(args.node, here)
    try:
        for b, t in enumerate(traces, 1):
            print(f'\n[{b}/{len(traces)}] {t["key"]}  set={t.get("set", "")}  group={t.get("group", "")}'
                  f'  b={t.get("b_pod")}  peak={max(t["cpu_trace"]) * 1000:.0f}m', flush=True)
            for attempt in (1, 2):
                try:
                    summ, steps = run_batch(t, policies, args, sampler, run)
                    break
                except RuntimeError as e:
                    print(f'    [WARN] attempt {attempt} failed: {e}', flush=True)
                    delete_all()
                    if attempt == 2: summ, steps = [], []
            append_csv(os.path.join(out, 'summary.csv'), summ)
            append_csv(os.path.join(out, 'steps.csv'), steps)
            print_summary(summ)
            if args.push and summ:
                git_push(f'results {run} {t["key"]}')
    except KeyboardInterrupt:
        print('\nInterrupted - cleaning up', flush=True)
    finally:
        try: delete_all(wait=False)
        except Exception: pass
        if proc: proc.terminate()
    print(f'\nDone. Results in {out}/summary.csv and steps.csv')

REPLAYER_CODE = r'''#!/usr/bin/env python3
"""
replayer.py - reproduces one pod's CPU trace inside a Kubernetes container.

Runs in the stock python:3.12-slim image (mounted from a ConfigMap, no image
build needed). Environment:
  TRACE     comma-separated CPU demand per step, in cores (e.g. 0.083,0.091,...)
  STEP_S    step length in seconds (5)
  START_AT  Unix time at which step 0 begins (shared by all pods of a batch)

Method: every step is split into 100-ms slots (= the CFS period). In each slot
the process consumes demand x 100 ms of CPU TIME, measured with
time.thread_time(), and then sleeps until the slot ends. Using CPU time
(not wall time) makes the delivered CPU independent of other pods sharing
the core. Each pod also starts its slots at a random phase offset, so
co-located pods do not all compete at the same instant.
If the container is throttled (demand > limit), the kernel stops it for the
rest of the CFS period; the slot then ends with less CPU consumed, and the
unserved demand is lost, not carried over - the same open-loop assumption as
the simulator. A single thread can use at most 1 core (demand capped at 1.0).
"""
import os, random, sys, time

SLOT = 0.1  # s, equal to the default CFS period (100 ms)

def main():
    trace = [min(max(float(x), 0.0), 1.0) for x in os.environ['TRACE'].split(',') if x]
    step = float(os.environ.get('STEP_S', '5'))
    start = float(os.environ['START_AT'])
    slots = int(round(step / SLOT))
    phase = random.random() * SLOT          # de-synchronise co-located pods

    wait = start - time.time()
    print(f'replayer: {len(trace)} steps, start in {wait:.1f}s, phase {phase*1000:.0f} ms',
          flush=True)
    if wait > 0:
        time.sleep(wait)
    elif wait < -step:
        print('replayer: WARNING started more than one step late', flush=True)

    for k, c in enumerate(trace):
        t_step = start + k * step
        target = c * SLOT                    # CPU seconds to consume per slot
        for j in range(slots):
            t0 = t_step + j * SLOT + phase
            t_end = t0 + SLOT
            now = time.time()
            if now >= t_end:                 # slot already over (late) -> skip
                continue
            if now < t0:
                time.sleep(t0 - now)
            if target <= 0:
                continue
            c0 = time.thread_time()
            # spin until this slot's CPU time is consumed, or the slot ends
            while time.thread_time() - c0 < target and time.time() < t_end:
                pass
        if k % 12 == 0:
            print(f'replayer: step {k} demand {c*1000:.1f}m', flush=True)

    print('replayer: trace finished, idling', flush=True)
    while True:            # keep the container alive until the controller deletes it
        time.sleep(3600)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
'''

TRACE_A_S1 = json.loads('{"key":"A_S1","dataset":"A","set":"stress","container_id":"c_10351","workload":"app_5279","group":"S","b_pod":0.0794,"scale":0.04830917874396136,"window_offset_steps":120,"max_rise":2.0,"window_start_s":600.0,"step_s":5.0,"steps":96,"w_warm":12,"control_every":3,"min_obs":4,"change_thresh":0.1,"p1_margin":1.15,"default_b":0.5,"pool_cap":600,"cold_request":0.13913,"cpu_trace":[0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913,0.13913],"pooled_seed":[0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638,0.04638]}')

def _synthetic(key, trace, note):
    """Synthetic check trace with A_S1's controller settings and history seed."""
    t = dict(TRACE_A_S1)
    mx = max(trace); mn = sum(trace) / len(trace)
    t.update(key=key, set='synthetic', container_id=None, workload=note, group='SYN',
             b_pod=round((mx - mn) / mx, 4), cpu_trace=[round(v, 5) for v in trace])
    return t

_LO, _HI = 0.04638, 0.13913        # A_S1 base and peak demand (cores)
# gradual drift: flat for 12 steps, then linear ramp from base to peak by the last step
_DRIFT = [_LO if k < 12 else _LO + (_HI - _LO) * (k - 12) / 83 for k in range(96)]
# bursty: base demand with 3-step bursts to peak every 12 steps (from step 18)
_BURST = [_HI if k >= 18 and (k - 18) % 12 < 3 else _LO for k in range(96)]
EMBEDDED_TRACES = [TRACE_A_S1,
                   _synthetic('A_SYN_DRIFT', _DRIFT, 'synthetic gradual ramp'),
                   _synthetic('A_SYN_BURST', _BURST, 'synthetic periodic bursts')]

if __name__ == '__main__':
    main()
