package model

import (
	"math"
	"time"

	"vpacheck/util"
)

const MaxResourceAmount = ResourceAmount(1e14)

// CPUUnit: amounts per core. 1000 = the real VPA (integer millicores). The ONLY deliberate deviation from the VPA
// source: set to 1e9 ("hires" mode) to remove integer truncation and test the algorithm alone.
var CPUUnit = 1000.0

var _ = math.Pi

type ResourceName string
type ResourceAmount int64
type Resources map[ResourceName]ResourceAmount

const (
	ResourceCPU ResourceName = "cpu"
	ResourceMemory ResourceName = "memory"
)

// CPUAmountFromCores converts CPU cores to a ResourceAmount.
func CPUAmountFromCores(cores float64) ResourceAmount {
	return resourceAmountFromFloat(cores * CPUUnit)
}

// CoresFromCPUAmount converts ResourceAmount to number of cores expressed as float64.
func CoresFromCPUAmount(cpuAmount ResourceAmount) float64 {
	return float64(cpuAmount) / CPUUnit
}

// MemoryAmountFromBytes converts memory bytes to a ResourceAmount.
func MemoryAmountFromBytes(bytes float64) ResourceAmount {
	return resourceAmountFromFloat(bytes)
}

// ScaleResource returns the resource amount multiplied by a given factor.
func ScaleResource(amount ResourceAmount, factor float64) ResourceAmount {
	return resourceAmountFromFloat(float64(amount) * factor)
}

// ResourceAmountMax returns the larger of two resource amounts.
func ResourceAmountMax(amount1, amount2 ResourceAmount) ResourceAmount {
	if amount1 > amount2 {
		return amount1
	}
	return amount2
}

func resourceAmountFromFloat(amount float64) ResourceAmount {
	if amount > float64(MaxResourceAmount) {
		return MaxResourceAmount
	}

	if amount < 0 {
		amount = 0
	}

	return ResourceAmount(amount)
}


// minSampleWeight: verbatim from aggregations_config.go (1.8.0)
const minSampleWeight = 0.1

// AggregateContainerState: only the fields the CPU estimators read.
type AggregateContainerState struct {
	AggregateCPUUsage    util.Histogram
	AggregateMemoryPeaks util.Histogram
	FirstSampleStart     time.Time
	LastSampleStart      time.Time
	TotalSamplesCount    int
}

type ContainerUsageSample struct {
	MeasureStart time.Time
	Usage        ResourceAmount
}

// addCPUSample: verbatim from aggregate_container_state.go (1.8.0)
func (a *AggregateContainerState) AddCPUSample(sample *ContainerUsageSample) {
	cpuUsageCores := CoresFromCPUAmount(sample.Usage)
	a.AggregateCPUUsage.AddSample(
		cpuUsageCores, minSampleWeight, sample.MeasureStart)
	if sample.MeasureStart.After(a.LastSampleStart) {
		a.LastSampleStart = sample.MeasureStart
	}
	if a.FirstSampleStart.IsZero() || sample.MeasureStart.Before(a.FirstSampleStart) {
		a.FirstSampleStart = sample.MeasureStart
	}
	a.TotalSamplesCount++
}
