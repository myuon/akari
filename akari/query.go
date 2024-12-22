package akari

import (
	"fmt"
	"log"
	"slices"
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
	Name     string
	From     string
	Function QueryFunction
	Filter   *QueryFilter
}

func (a Query) Apply(columns LogRecordColumns, records LogRecordRows) (any, error) {
	fromIndex := columns.GetIndex(a.From)
	valueType := columns[columns.GetIndex(a.From)].Type

	switch valueType {
	case LogRecordTypeInt:
		values := GetLogRecordsNumbers[int](records, fromIndex)
		values = apply(a.Filter, values)

		return evaluate(a.Function, values), nil
	case LogRecordTypeInt64:
		fallthrough
	case LogRecordTypeFloat64:
		values := GetLogRecordsNumbers[float64](records, fromIndex)
		values = apply(a.Filter, values)

		return evaluate(a.Function, values), nil
	case LogRecordTypeString:
		values := records.GetStrings(fromIndex)

		switch a.Function {
		case QueryFunctionCount:
			return len(values), nil
		case QueryFunctionAny:
			return values[0], nil
		default:
			return nil, fmt.Errorf("Unknown function: %v", a.Function)
		}
	default:
		return nil, fmt.Errorf("Unknown value type: %v", valueType)
	}
}
