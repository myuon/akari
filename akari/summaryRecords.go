package akari

import (
	"fmt"
	"slices"
)

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

func (r *SummaryRecords) Insert(at int, column SummaryRecordColumn, generator func(key string, row []any) any) {
	r.Columns = append(r.Columns[:at], append([]SummaryRecordColumn{column}, r.Columns[at:]...)...)

	for key := range r.Rows {
		r.Rows[key] = append(r.Rows[key][:at], append([]any{generator(key, r.Rows[key])}, r.Rows[key][at:]...)...)
	}
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

type FormatColumnOptions struct {
	Name      string
	Format    string
	Alignment string
}

type FormatOptions struct {
	ColumnOptions []FormatColumnOptions
	Limit         int
}

func (r SummaryRecordKeyPairs) Format(options FormatOptions) TableData {
	rows := [][]string{}
	for j, record := range r {
		if options.Limit > 0 && j > options.Limit {
			break
		}

		row := []string{}
		for i, value := range record.Record {
			format := options.ColumnOptions[i].Format
			if format == "" {
				format = "%v"
			}

			row = append(row, fmt.Sprintf(format, value))
		}

		rows = append(rows, row)
	}

	columns := []TableColumn{}
	for _, column := range options.ColumnOptions {
		columns = append(columns, TableColumn{
			Name:      column.Name,
			Alignment: column.Alignment,
		})
	}

	return TableData{
		Columns: columns,
		Rows:    rows,
	}
}
