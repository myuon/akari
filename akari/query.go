package akari

import (
	"log"
	"slices"
)

type QueryValueType string

const (
	QueryValueTypeInt     QueryValueType = "int"
	QueryValueTypeInt64   QueryValueType = "int64"
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

func evaluate[T int | float64](f QueryFunction, values []T) any {
	switch f {
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
		log.Fatalf("Unknown function: %v", f)
	}

	return 0
}

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

func applyRow[T int | float64](f QueryFilter, value T) bool {
	switch f.Type {
	case QueryFilterTypeBetween:
		return float64(value) >= f.Between.Start && float64(value) <= f.Between.End
	default:
		log.Fatalf("Unknown filter type: %v", f.Type)
	}

	return false
}

func apply[T int | float64](f *QueryFilter, values []T) []T {
	if f == nil {
		return values
	}

	filtered := []T{}
	for _, value := range values {
		if applyRow(*f, value) {
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
		values := GetLogRecordsNumbers[int](records, fromIndex)
		values = apply(a.Filter, values)

		return evaluate(a.Function, values)
	case QueryValueTypeInt64:
		fallthrough
	case QueryValueTypeFloat64:
		values := GetLogRecordsNumbers[float64](records, fromIndex)
		values = apply(a.Filter, values)

		return evaluate(a.Function, values)
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
