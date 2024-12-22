package akari

type LogRecordColumn struct {
	Name string
}

type LogRecordRow []any
type LogRecordRows []LogRecordRow

type LogRecords struct {
	Columns    []LogRecordColumn
	KeyColumns []LogRecordColumn
	Records    map[string]LogRecordRows
}

func (r LogRecords) GetIndex(key string) int {
	for i, column := range r.Columns {
		if column.Name == key {
			return i
		}
	}

	return -1
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
