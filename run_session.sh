#!/usr/bin/env bash
# run_session.sh — one Killercoda session = one part of one run.
#   ./run_session.sh A 1/4 r1            # Dataset A, first quarter of 14 traces, run r1 (~38 min)
#   ./run_session.sh B 2/4 r2
# Extra arguments are passed to k8s_prototype.py (e.g. --policies P1,P2,P3,P6,P7,P8).
# Results are pushed to git after EVERY trace; rerunning the same command in a
# new session skips traces that are already finished.
set -u
DS=${1:-A}; PART=${2:-1/4}; RUN=${3:-r1}; shift $(( $# < 3 ? $# : 3 ))
cd "$(dirname "$0")"
kubectl get nodes >/dev/null || { echo "kubectl not working"; exit 1; }
[ -f traces.json ] || { echo "traces.json missing - run export_traces.py on Windows and push it"; exit 1; }
mkdir -p results
python3 -u k8s_prototype.py --traces traces.json --datasets "$DS" --part "$PART" \
        --run "$RUN" --out results --push "$@" 2>&1 \
    | tee -a "results/log_${RUN}_${DS}_${PART/\//of}.txt"
git add results && git -c user.name=k8s -c user.email=k8s@localhost \
    commit -qm "log $RUN $DS $PART" ; git pull --rebase -q && git push -q && echo "== results pushed =="
