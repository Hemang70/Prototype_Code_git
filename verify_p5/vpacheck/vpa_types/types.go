package v1

import metav1 "vpacheck/metav1"

type HistogramCheckpoint struct {
	ReferenceTimestamp metav1.Time
	BucketWeights      map[int]uint32
	TotalWeight        float64
}
