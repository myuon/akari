package akari

import "slices"

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

func (r SummaryRecords) GetKeyPairs() SummaryRecordKeyPairs {
	summaryRecords := []SummaryRecordKeyPair{}
	for key, record := range r.Rows {
		summaryRecords = append(summaryRecords, SummaryRecordKeyPair{
			Key:    key,
			Record: record,
		})
	}

	return summaryRecords
}

type SummaryRecordKeyPairs []SummaryRecordKeyPair

func (r *SummaryRecordKeyPairs) SortBy(sortKeys []int) {
	records := *r

	slices.SortStableFunc([]SummaryRecordKeyPair(records), func(a, b SummaryRecordKeyPair) int {
		for _, sortKey := range sortKeys {
			if a.Record[sortKey].(float64) > b.Record[sortKey].(float64) {
				return -1
			} else if a.Record[sortKey].(float64) < b.Record[sortKey].(float64) {
				return 1
			}
		}

		return 0
	})

	*r = records
}
