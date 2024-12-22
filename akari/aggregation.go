package akari

import "log"

type AggregationValueType string

const (
	AggregationValueTypeInt     AggregationValueType = "int"
	AggregationValueTypeFloat64 AggregationValueType = "float64"
)

type AggregationFunction string

const (
	AggregationFunctionCount AggregationFunction = "count"
	AggregationFunctionSum   AggregationFunction = "sum"
	AggregationFunctionAny   AggregationFunction = "any"
)

type AggregationFilterType string

const (
	AggregationFilterTypeBetween AggregationFilterType = "between"
)

type AggregationFilter struct {
	Type    AggregationFilterType
	Between struct {
		Min float64
		Max float64
	}
}

func (f AggregationFilter) ApplyRowInt(value int) bool {
	switch f.Type {
	case AggregationFilterTypeBetween:
		return value >= int(f.Between.Min) && value <= int(f.Between.Max)
	default:
		log.Fatalf("Unknown filter type: %v", f.Type)
	}

	return false
}

func (f AggregationFilter) ApplyRowFloat64(value float64) bool {
	switch f.Type {
	case AggregationFilterTypeBetween:
		return value >= f.Between.Min && value <= f.Between.Max
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
		default:
			log.Fatalf("Unknown function: %v", a.Function)
		}
	default:
		log.Fatalf("Unknown value type: %v", a.ValueType)
	}

	return nil
}
