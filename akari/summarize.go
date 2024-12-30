package akari

import "fmt"

type LogRecordType string

const (
	LogRecordTypeInt      LogRecordType = "int"
	LogRecordTypeInt64    LogRecordType = "int64"
	LogRecordTypeFloat64  LogRecordType = "float64"
	LogRecordTypeString   LogRecordType = "string"
	LogRecordTypeDateTime LogRecordType = "datetime"
)

func (t LogRecordType) IsFloat() bool {
	return t == LogRecordTypeFloat64
}

func (t LogRecordType) IsNumeric() bool {
	return t == LogRecordTypeInt || t == LogRecordTypeInt64 || t == LogRecordTypeFloat64
}

type LogRecordColumn struct {
	Name string
	Type LogRecordType
}

type LogRecordColumns []LogRecordColumn

func (c LogRecordColumns) GetIndex(key string) int {
	for i, column := range c {
		if column.Name == key {
			return i
		}
	}

	return -1
}

type LogRecordRow []any
type LogRecordRows []LogRecordRow

type LogRecords struct {
	Columns LogRecordColumns
	Records map[string]LogRecordRows
}

func GetLogRecordsNumbers[T int | float64](records LogRecordRows, index int) []T {
	numbers := []T{}
	for _, record := range records {
		numbers = append(numbers, record[index].(T))
	}

	return numbers
}

func (r LogRecordRows) GetFloats(index int) []float64 {
	floats := []float64{}
	for _, record := range r {
		floats = append(floats, record[index].(float64))
	}

	return floats
}

func (r LogRecordRows) GetInts(index int) []int {
	ints := []int{}
	for _, record := range r {
		ints = append(ints, record[index].(int))
	}

	return ints
}

func (r LogRecordRows) GetStrings(index int) []string {
	strings := []string{}
	for _, record := range r {
		strings = append(strings, record[index].(string))
	}

	return strings
}

func (r LogRecords) Summarize(queries []Query, prevRows map[string]LogRecordRows) (SummaryRecords, error) {
	summary := map[string][]SummaryRowCell{}
	resultTypes := map[string]LogRecordType{}
	for key, records := range r.Records {
		row := []SummaryRowCell{}
		for _, query := range queries {
			value, resultType, err := query.Apply(r.Columns, records)
			if err != nil {
				return SummaryRecords{}, fmt.Errorf("Failed to apply query: %v (cause: %w)", query, err)
			}

			row = append(row, SummaryRowCell{
				Value: value,
			})
			resultTypes[query.Name] = resultType
		}

		summary[key] = row
	}

	for prevKey, prevRow := range prevRows {
		for k, query := range queries {
			value, _, err := query.Apply(r.Columns, prevRow)
			if err != nil {
				return SummaryRecords{}, fmt.Errorf("Failed to apply query: %v (cause: %w)", query, err)
			}

			if row, ok := summary[prevKey]; ok {
				row[k].PrevValue = value
			}
		}
	}

	columns := []SummaryRecordColumn{}
	for _, q := range queries {
		columns = append(columns, SummaryRecordColumn{
			Name: q.Name,
			Type: resultTypes[q.Name],
		})
	}

	return SummaryRecords{
		Columns: columns,
		Rows:    summary,
	}, nil
}
