package metav1

import "time"

type Time struct{ time.Time }

func NewTime(t time.Time) Time { return Time{t} }
