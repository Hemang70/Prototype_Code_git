# Running the real VPA 1.8.0 recommender in the cluster and comparing it with P5

Place this folder as `Prototype_Code_git/verify_p5/` (so `../../k8s_prototype.py` resolves). Python 3 + kubectl only.

    cd verify_p5/live
    python3 p5_live_check.py selftest          # 5 s, no cluster: proves the comparison code works (456/456)
    python3 p5_live_check.py install           # metrics-server (if missing) + VPA 1.8.0 CRDs, RBAC, recommender
    python3 p5_live_check.py run --minutes 35  # ~40 min; replays a piecewise-constant CPU pattern in a Deployment
    python3 p5_live_check.py compare live_run_<timestamp>
    python3 p5_live_check.py cleanup --all

Run it on its own: `node01` has 1 CPU, so do not run other experiment sessions at the same time.
Only the recommender is installed (VPA `updateMode: Off`); the updater and admission controller are not needed
because the thing being compared is the recommendation.  Manifests in `manifests/` are the unmodified files from
tag `vertical-pod-autoscaler-1.8.0`.

## What is compared
* Check 1 (exact, the one to quote): the recommender writes its CPU histogram to a `VerticalPodAutoscalerCheckpoint`.
  That histogram is loaded into P5's `VPAModel`; P5's own `target()` and `bounds()` must reproduce the target, lower and
  upper bound that the real VPA published in the VPA object, in every recommender loop. Allowed difference: VPA's
  integer-millicore flooring (derived bound) and the checkpoint's weight rounding (10000 steps).
* Check 2 (informational): P5 fed with metrics-server usage at the recommender's sample times. The recommender chooses its
  own sampling instants, so expect agreement within about one 5 % bucket, not exactly.

## Honest status
* Written and tested without access to your cluster. Tested: the comparison code against checkpoint data produced by the
  real VPA histogram code (456 snapshots reproduced) and against fake API-shaped run files. NOT tested: kubectl calls,
  image pulls, metrics-server install on your cluster. Field names follow the 1.8.0 CRD.
* Sensitivity: five deliberately wrong P5 variants (margin 0.20, target percentile 0.85, upper percentile 0.99,
  confidence interval 12 h, minimum 50 m) are all rejected by `selftest`; see ../NEGATIVE_CONTROLS.txt.
* If Check 1 fails, paste the FAIL lines and `meta.json`; the first loops (few samples, huge upper bound) are the usual suspects.
