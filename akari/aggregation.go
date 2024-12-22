package akari

import (
	"log"
	"slices"
)

type AggregationValueType string

const (
	AggregationValueTypeInt     AggregationValueType = "int"
	AggregationValueTypeFloat64 AggregationValueType = "float64"
	AggregationValueTypeString  AggregationValueType = "string"
)

type AggregationFunction string

const (
	AggregationFunctionCount  AggregationFunction = "count"
	AggregationFunctionSum    AggregationFunction = "sum"
	AggregationFunctionMean   AggregationFunction = "mean"
	AggregationFunctionMax    AggregationFunction = "max"
	AggregationFunctionMin    AggregationFunction = "min"
	AggregationFunctionStddev AggregationFunction = "stddev"
	AggregationFunctionP50    AggregationFunction = "p50"
	AggregationFunctionP90    AggregationFunction = "p90"
	AggregationFunctionP95    AggregationFunction = "p95"
	AggregationFunctionP99    AggregationFunction = "p99"
	AggregationFunctionAny    AggregationFunction = "any"
)

type AggregationFilterType string

const (
	AggregationFilterTypeBetween AggregationFilterType = "between"
)

type AggregationFilter struct {
	Type    AggregationFilterType
	Between struct {
		Start float64
		End   float64
	}
}

func (f AggregationFilter) ApplyRowInt(value int) bool {
	switch f.Type {
	case AggregationFilterTypeBetween:
		return value >= int(f.Between.Start) && value <= int(f.Between.End)
	default:
		log.Fatalf("Unknown filter type: %v", f.Type)
	}

	return false
}

func (f AggregationFilter) ApplyRowFloat64(value float64) bool {
	switch f.Type {
	case AggregationFilterTypeBetween:
		return value >= f.Between.Start && value <= f.Between.End
	default:
		log.Fatalf("Unknown filter type: %v", f.Type)
	}

	return false
}

func (f *AggregationFilter) ApplyInt(values []int) []int {
	if f == nil {
		return values
	}

	filtered := []int{}
	for _, value := range values {
		if f.ApplyRowInt(value) {
			filtered = append(filtered, value)
		}
	}

	return filtered
}

func (f *AggregationFilter) ApplyFloat64(values []float64) []float64 {
	if f == nil {
		return values
	}

	filtered := []float64{}
	for _, value := range values {
		if f.ApplyRowFloat64(value) {
			filtered = append(filtered, value)
		}
	}

	return filtered
}

type Aggregation struct {
	Name      string
	From      string
	ValueType AggregationValueType
	Function  AggregationFunction
	Filter    *AggregationFilter
}

func (a Aggregation) Apply(columns LogRecordColumns, records LogRecordRows) any {
	fromIndex := columns.GetIndex(a.From)

	switch a.ValueType {
	case AggregationValueTypeInt:
		values := records.GetInts(fromIndex)
		values = a.Filter.ApplyInt(values)

		switch a.Function {
		case AggregationFunctionCount:
			return len(values)
		case AggregationFunctionSum:
			return GetSum(values)
		case AggregationFunctionAny:
			return values[0]
		case AggregationFunctionMean:
			return GetMean(values)
		case AggregationFunctionStddev:
			return GetStddev(values)
		case AggregationFunctionMax:
			return slices.Max(values)
		case AggregationFunctionMin:
			return slices.Min(values)
		case AggregationFunctionP50:
			return GetPercentile(values, 50)
		case AggregationFunctionP90:
			return GetPercentile(values, 90)
		case AggregationFunctionP95:
			return GetPercentile(values, 95)
		case AggregationFunctionP99:
			return GetPercentile(values, 99)
		default:
			log.Fatalf("Unknown function: %v", a.Function)
		}
	case AggregationValueTypeFloat64:
		values := records.GetFloats(fromIndex)
		values = a.Filter.ApplyFloat64(values)

		switch a.Function {
		case AggregationFunctionCount:
			return len(values)
		case AggregationFunctionSum:
			return GetSum(values)
		case AggregationFunctionAny:
			return values[0]
		case AggregationFunctionMean:
			return GetMean(values)
		case AggregationFunctionStddev:
			return GetStddev(values)
		case AggregationFunctionMax:
			return slices.Max(values)
		case AggregationFunctionMin:
			return slices.Min(values)
		case AggregationFunctionP50:
			return GetPercentile(values, 50)
		case AggregationFunctionP90:
			return GetPercentile(values, 90)
		case AggregationFunctionP95:
			return GetPercentile(values, 95)
		case AggregationFunctionP99:
			return GetPercentile(values, 99)
		default:
			log.Fatalf("Unknown function: %v", a.Function)
		}
	case AggregationValueTypeString:
		values := records.GetStrings(fromIndex)

		switch a.Function {
		case AggregationFunctionCount:
			return len(values)
		case AggregationFunctionAny:
			return values[0]
		default:
			log.Fatalf("Unknown function: %v", a.Function)
		}
	default:
		log.Fatalf("Unknown value type: %v", a.ValueType)
	}

	return nil
}
