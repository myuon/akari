package akari

type LogRecordColumn struct {
	Name string
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

func (r LogRecords) Summarize(queries []Query) SummaryRecords {
	summary := map[string][]any{}
	for key, records := range r.Records {
		row := []any{}
		for _, query := range queries {
			row = append(row, query.Apply(r.Columns, records))
		}

		summary[key] = row
	}

	columns := []SummaryRecordColumn{}
	for _, q := range queries {
		columns = append(columns, SummaryRecordColumn{Name: q.Name})
	}

	return SummaryRecords{
		Columns: columns,
		Rows:    summary,
	}
}
