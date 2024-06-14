package model

import "time"

type MetricType int

const (
	Gauge MetricType = iota
	Count
	Summary
)

// Metric model
type Metric struct {
	Name     string
	Type     MetricType
	Value    Numeric
	Interval time.Duration
	Timestamp int64
	Attributes map[string]interface{}
	//TODO: summary metric data model
}

// Make a gauge metric.
func NewGaugeMetric(name string, value Numeric, timestamp time.Time) Metric {
	return Metric{
		Name:  name,
		Type:  Gauge,
		Value: value,
		Timestamp: timestamp.UnixMilli(),
		Attributes: make(map[string]interface{}),
	}
}

// Make a count metric. If interval is 0 the counter is cumulative. Otherwise is delta.
func NewCountMetric(name string, value Numeric, interval time.Duration, timestamp time.Time) Metric {
	return Metric{
		Name:     name,
		Type:     Count,
		Value:    value,
		Interval: interval,
		Timestamp: timestamp.UnixMilli(),
		Attributes: make(map[string]interface{}),
	}
}

// Numeric model.
type Numeric struct {
	IntOrFlt bool // true = Int, false = Float
	IntVal   int64
	FltVal   float64
}

// Numeric holds an integer.
func (n *Numeric) IsInt() bool {
	return n.IntOrFlt
}

// Numeric holds a float.
func (n *Numeric) IsFloat() bool {
	return !n.IntOrFlt
}

// Get float from Numeric.
func (n *Numeric) Float() float64 {
	if n.IsFloat() {
		return n.FltVal
	} else {
		return float64(n.IntVal)
	}
}

// Get int from Numeric.
func (n *Numeric) Int() int64 {
	if n.IsInt() {
		return n.IntVal
	} else {
		return int64(n.FltVal)
	}
}

// Get whatever it is.
func (n *Numeric) Value() any {
	if n.IsInt() {
		return n.IntVal
	} else {
		return n.FltVal
	}
}

// Make a Numeric from either an int, int64 or float64. Other types will cause a panic.
func MakeNumeric(val any) Numeric {
	switch val := val.(type) {
	case int:
		return Numeric{
			IntOrFlt: true,
			IntVal:   int64(val),
			FltVal:   0.0,
		}
	case int64:
		return Numeric{
			IntOrFlt: true,
			IntVal:   val,
			FltVal:   0.0,
		}
	case float64:
		return Numeric{
			IntOrFlt: false,
			IntVal:   0,
			FltVal:   val,
		}
	default:
		panic("Numeric must be either an int, int64 or float64")
	}
}

//TODO: make summary metric
