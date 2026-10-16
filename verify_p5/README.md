# P5 vs Kubernetes VPA 1.8.0 - verification kit

1. `p5_verify.py` (no cluster)  - P5 vs the unmodified VPA 1.8.0 histogram/estimator code. See header and UPSTREAM_DIFF.txt.
   Result 2026-10-16: PASS, 49/49 cases (hires: <= 0.00001 %; shipped: inside VPA's own integer-truncation bound).
       python3 p5_verify.py --traces ../traces.json --proto ../k8s_prototype.py --bin ./vpacheck-linux-amd64
2. `live/p5_live_check.py` (cluster) - P5 vs the real recommender process running in Kubernetes. See live/README.md.
3. NEGATIVE_CONTROLS.txt - evidence that both checks reject a P5 with a wrong constant.

Not covered by either check: the updater's "older than 12 h and change >= 10 %" rule, eviction limits (min-replicas),
recommender->updater delay, the 1.15 limit/request ratio (experiment assumption), and 1-minute mean vs instantaneous sampling.
