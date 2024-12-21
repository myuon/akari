package akari

type LogRecordColumn struct {
	Name string
}

type LogRecords struct {
	Columns    []LogRecordColumn
	KeyColumns []LogRecordColumn
	Records    map[string][][]any
}

func (r LogRecords) GetIndex(key string) int {
	for i, column := range r.Columns {
		if column.Name == key {
			return i
		}
	}

	return -1
}
