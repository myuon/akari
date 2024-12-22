package akari

import (
	"log"
	"slices"
)

type QueryValueType string

const (
	QueryValueTypeInt     QueryValueType = "int"
	QueryValueTypeFloat64 QueryValueType = "float64"
	QueryValueTypeString  QueryValueType = "string"
)

type QueryFunction string

const (
	QueryFunctionCount  QueryFunction = "count"
	QueryFunctionSum    QueryFunction = "sum"
	QueryFunctionMean   QueryFunction = "mean"
	QueryFunctionMax    QueryFunction = "max"
	QueryFunctionMin    QueryFunction = "min"
	QueryFunctionStddev QueryFunction = "stddev"
	QueryFunctionP50    QueryFunction = "p50"
	QueryFunctionP90    QueryFunction = "p90"
	QueryFunctionP95    QueryFunction = "p95"
	QueryFunctionP99    QueryFunction = "p99"
	QueryFunctionAny    QueryFunction = "any"
)

type QueryFilterType string

const (
	QueryFilterTypeBetween QueryFilterType = "between"
)

type QueryFilter struct {
	Type    QueryFilterType
	Between struct {
		Start float64
		End   float64
	}
}

func (f QueryFilter) ApplyRowInt(value int) bool {
	switch f.Type {
	case QueryFilterTypeBetween:
		return value >= int(f.Between.Start) && value <= int(f.Between.End)
	default:
		log.Fatalf("Unknown filter type: %v", f.Type)
	}

	return false
}

func (f QueryFilter) ApplyRowFloat64(value float64) bool {
	switch f.Type {
	case QueryFilterTypeBetween:
		return value >= f.Between.Start && value <= f.Between.End
	default:
		log.Fatalf("Unknown filter type: %v", f.Type)
	}

	return false
}

func (f *QueryFilter) ApplyInt(values []int) []int {
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

func (f *QueryFilter) ApplyFloat64(values []float64) []float64 {
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

type Query struct {
	Name      string
	From      string
	ValueType QueryValueType
	Function  QueryFunction
	Filter    *QueryFilter
}

func (a Query) Apply(columns LogRecordColumns, records LogRecordRows) any {
	fromIndex := columns.GetIndex(a.From)

	switch a.ValueType {
	case QueryValueTypeInt:
		values := records.GetInts(fromIndex)
		values = a.Filter.ApplyInt(values)

		switch a.Function {
		case QueryFunctionCount:
			return len(values)
		case QueryFunctionSum:
			return GetSum(values)
		case QueryFunctionAny:
			return values[0]
		case QueryFunctionMean:
			return GetMean(values)
		case QueryFunctionStddev:
			return GetStddev(values)
		case QueryFunctionMax:
			return slices.Max(values)
		case QueryFunctionMin:
			return slices.Min(values)
		case QueryFunctionP50:
			return GetPercentile(values, 50)
		case QueryFunctionP90:
			return GetPercentile(values, 90)
		case QueryFunctionP95:
			return GetPercentile(values, 95)
		case QueryFunctionP99:
			return GetPercentile(values, 99)
		default:
			log.Fatalf("Unknown function: %v", a.Function)
		}
	case QueryValueTypeFloat64:
		values := records.GetFloats(fromIndex)
		values = a.Filter.ApplyFloat64(values)

		switch a.Function {
		case QueryFunctionCount:
			return len(values)
		case QueryFunctionSum:
			return GetSum(values)
		case QueryFunctionAny:
			return values[0]
		case QueryFunctionMean:
			return GetMean(values)
		case QueryFunctionStddev:
			return GetStddev(values)
		case QueryFunctionMax:
			return slices.Max(values)
		case QueryFunctionMin:
			return slices.Min(values)
		case QueryFunctionP50:
			return GetPercentile(values, 50)
		case QueryFunctionP90:
			return GetPercentile(values, 90)
		case QueryFunctionP95:
			return GetPercentile(values, 95)
		case QueryFunctionP99:
			return GetPercentile(values, 99)
		default:
			log.Fatalf("Unknown function: %v", a.Function)
		}
	case QueryValueTypeString:
		values := records.GetStrings(fromIndex)

		switch a.Function {
		case QueryFunctionCount:
			return len(values)
		case QueryFunctionAny:
			return values[0]
		default:
			log.Fatalf("Unknown function: %v", a.Function)
		}
	default:
		log.Fatalf("Unknown value type: %v", a.ValueType)
	}

	return nil
}
