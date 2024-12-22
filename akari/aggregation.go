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

type Aggregation struct {
	Name      string
	From      string
	ValueType AggregationValueType
	Function  AggregationFunction
}

func (a Aggregation) Apply(columns LogRecordColumns, records LogRecordRows) any {
	switch a.Function {
	case AggregationFunctionCount:
		return len(records)
	case AggregationFunctionSum:
		fromIndex := columns.GetIndex(a.From)

		switch a.ValueType {
		case AggregationValueTypeInt:
			return GetSum(records.GetInts(fromIndex))
		case AggregationValueTypeFloat64:
			return GetSum(records.GetFloats(fromIndex))
		default:
			log.Fatalf("Unknown value type: %v", a.ValueType)
		}
	case AggregationFunctionAny:
		fromIndex := columns.GetIndex(a.From)

		return records[0][fromIndex]
	default:
		log.Fatalf("Unknown function: %v", a.Function)
	}

	return nil
}
