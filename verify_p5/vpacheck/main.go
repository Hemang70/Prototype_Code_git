// vpacheck: runs the *unmodified* VPA 1.8.0 histogram + estimator code on a sample stream and prints
// target / lowerBound / upperBound (millicores) at requested times.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"vpacheck/logic"
	"vpacheck/model"
	"vpacheck/util"
)

type In struct {
	Samples [][2]float64 `json:"samples"` // [t_seconds, cores] one per minute (VPA metrics fetch interval)
	Queries []float64    `json:"queries"` // t_seconds
	Unit    float64      `json:"unit"`    // 0/1000 = real VPA millicores; 1e9 = hires (no truncation)
}

func main() {
	var in In
	if err := json.NewDecoder(os.Stdin).Decode(&in); err != nil {
		panic(err)
	}
	if in.Unit > 0 {
		model.CPUUnit = in.Unit
	}
	// aggregations_config.go: NewExponentialHistogramOptions(1000.0, 0.01, 1.05, epsilon=0.001*0.1); half-life 24h
	opts, err := util.NewExponentialHistogramOptions(1000.0, 0.01, 1.+0.05, 0.001*0.1)
	if err != nil {
		panic(err)
	}
	st := &model.AggregateContainerState{
		AggregateCPUUsage:    util.NewDecayingHistogram(opts, 24*time.Hour),
		AggregateMemoryPeaks: util.NewDecayingHistogram(opts, 24*time.Hour),
	}
	// recommender.go CreatePodResourceRecommender (1.8.0) + config defaults (config.go)
	margin, conf := 0.15, 24*time.Hour
	target := logic.WithCPUMargin(margin, logic.NewPercentileCPUEstimator(0.9))
	lower := logic.WithCPUConfidenceMultiplier(0.001, -2.0, logic.WithCPUMargin(margin, logic.NewPercentileCPUEstimator(0.5)), conf)
	upper := logic.WithCPUConfidenceMultiplier(1.0, 1.0, logic.WithCPUMargin(margin, logic.NewPercentileCPUEstimator(0.95)), conf)
	minCPU := model.ScaleResource(model.CPUAmountFromCores(25*0.001), 1.0) // PodMinCPUMillicores=25, 1 container
	target = logic.WithCPUMinResource(minCPU, target)
	lower = logic.WithCPUMinResource(minCPU, lower)
	upper = logic.WithCPUMinResource(minCPU, upper)

	base := time.Unix(1_700_000_000, 0)
	type row struct {
		T, Target, Lower, Upper float64
		// real SaveToChekpoint() output + sample bookkeeping (what the recommender writes to the cluster)
		BucketWeights map[int]uint32
		TotalWeight   float64
		First, Last   float64 // seconds since base
		Count         int
	}
	var out []row
	si := 0
	for _, q := range in.Queries {
		for si < len(in.Samples) && in.Samples[si][0] <= q {
			s := in.Samples[si]
			st.AddCPUSample(&model.ContainerUsageSample{
				MeasureStart: base.Add(time.Duration(s[0] * float64(time.Second))),
				Usage:        model.CPUAmountFromCores(s[1]), // VPA stores usage as integer millicores
			})
			si++
		}
		ck, _ := st.AggregateCPUUsage.SaveToChekpoint()
		out = append(out, row{q, float64(target.GetCPUEstimation(st)), float64(lower.GetCPUEstimation(st)), float64(upper.GetCPUEstimation(st)),
			ck.BucketWeights, ck.TotalWeight, st.FirstSampleStart.Sub(base).Seconds(), st.LastSampleStart.Sub(base).Seconds(), st.TotalSamplesCount})
	}
	json.NewEncoder(os.Stdout).Encode(out)
	_ = fmt.Sprint
}
