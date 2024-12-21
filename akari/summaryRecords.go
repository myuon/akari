package akari

type SummaryRecordColumn struct {
	Name string
}

type SummaryRecords struct {
	Columns []SummaryRecordColumn
	Rows    map[string][]any
}

func (r SummaryRecords) GetIndex(key string) int {
	for i, column := range r.Columns {
		if column.Name == key {
			return i
		}
	}

	return -1
}

type SummaryRecordKeyPair struct {
	Key    string
	Record []any
}

func (r SummaryRecords) GetKeyPairs() []SummaryRecordKeyPair {
	summaryRecords := []SummaryRecordKeyPair{}
	for key, record := range r.Rows {
		summaryRecords = append(summaryRecords, SummaryRecordKeyPair{
			Key:    key,
			Record: record,
		})
	}

	return summaryRecords
}
