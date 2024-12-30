package akari

import (
	"fmt"
	"slices"
)

type SummaryRecordColumn struct {
	Name string
	Type LogRecordType
}

type SummaryRowCell struct {
	Value     any
	PrevValue any
}

type SummaryRecords struct {
	Columns []SummaryRecordColumn
	Rows    map[string][]SummaryRowCell
}

func (r SummaryRecords) GetIndex(key string) int {
	for i, column := range r.Columns {
		if column.Name == key {
			return i
		}
	}

	return -1
}

// func (r *SummaryRecords) Insert(at int, column SummaryRecordColumn, generator func(key string, row []any) any) {
// 	r.Columns = InsertAt(r.Columns, at, column)

// 	for key := range r.Rows {
// 		r.Rows[key] = InsertAt(r.Rows[key], at, generator(key, r.Rows[key]))
// 	}
// }

type SummaryRecordKeyPair struct {
	Key    string
	Record []SummaryRowCell
}

type SummaryRecordKeyPairs struct {
	Columns     []SummaryRecordColumn
	Entries     []SummaryRecordKeyPair
	PrevEntries []SummaryRecordKeyPair
}

func (r SummaryRecords) GetKeyPairs() SummaryRecordKeyPairs {
	entries := []SummaryRecordKeyPair{}
	for key, record := range r.Rows {
		entries = append(entries, SummaryRecordKeyPair{
			Key:    key,
			Record: record,
		})
	}

	return SummaryRecordKeyPairs{
		Columns: r.Columns,
		Entries: entries,
	}
}

func (r *SummaryRecordKeyPairs) SortBy(sortKeys []int) {
	records := *r

	slices.SortStableFunc(records.Entries, func(a, b SummaryRecordKeyPair) int {
		for _, sortKey := range sortKeys {
			if a.Record[sortKey].Value.(float64) > b.Record[sortKey].Value.(float64) {
				return -1
			} else if a.Record[sortKey].Value.(float64) < b.Record[sortKey].Value.(float64) {
				return 1
			}
		}

		return 0
	})

	*r = records
}

type FormatColumnOptions struct {
	Name          string
	Format        string
	Alignment     string
	HumanizeBytes bool
}

type FormatOptions struct {
	ColumnOptions []FormatColumnOptions
	Limit         int
}

func (r SummaryRecordKeyPairs) Format(options FormatOptions) TableData {
	rows := [][]TableCell{}
	for j, record := range r.Entries {
		if options.Limit > 0 && j > options.Limit {
			break
		}

		row := []TableCell{}
		for i, cell := range record.Record {
			format := options.ColumnOptions[i].Format
			if format == "" {
				if r.Columns[i].Type.IsFloat() {
					format = "%.3f"
				} else {
					format = "%v"
				}
			}
			if options.ColumnOptions[i].HumanizeBytes {
				cell.Value = HumanizeBytes(cell.Value.(int))
			}

			alignment := options.ColumnOptions[i].Alignment
			if alignment == "" {
				if r.Columns[i].Type.IsNumeric() {
					alignment = TableColumnAlignmentRight
				} else {
					alignment = TableColumnAlignmentLeft
				}
			}

			row = append(row, TableCell{
				Value:        fmt.Sprintf(format, cell.Value),
				RawValue:     cell.Value,
				PrevRawValue: cell.PrevValue,
				Alignment:    alignment,
			})
		}

		rows = append(rows, row)
	}

	columns := []TableColumn{}
	for k, column := range options.ColumnOptions {
		alignment := column.Alignment
		if alignment == "" {
			if r.Columns[k].Type.IsNumeric() {
				alignment = TableColumnAlignmentRight
			} else {
				alignment = TableColumnAlignmentLeft
			}
		}

		columns = append(columns, TableColumn{
			Name:      column.Name,
			Alignment: alignment,
		})
	}

	return TableData{
		Columns: columns,
		Rows:    rows,
	}
}
