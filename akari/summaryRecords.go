package akari

type SummaryRecordColumn struct {
	Name string
}

type SummaryRecord struct {
	Columns []SummaryRecordColumn
	Rows    map[string][]any
}

func (r SummaryRecord) GetIndex(key string) int {
	for i, column := range r.Columns {
		if column.Name == key {
			return i
		}
	}

	return -1
}
