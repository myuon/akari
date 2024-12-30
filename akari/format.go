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

type SummaryRecordKeyPair struct {
	Key    string
	Record []SummaryRowCell
}

type SummaryRecordKeyPairs struct {
	Columns []SummaryRecordColumn
	Entries []SummaryRecordKeyPair
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

type SortByOptions struct {
	SortKeyIndexes []int
	UsePrev        bool
}

func (r *SummaryRecordKeyPairs) SortBy(options SortByOptions) {
	records := *r

	slices.SortStableFunc(records.Entries, func(a, b SummaryRecordKeyPair) int {
		for _, sortKey := range options.SortKeyIndexes {
			valueA, _ := a.Record[sortKey].Value.(float64)
			if options.UsePrev {
				valueA, _ = a.Record[sortKey].PrevValue.(float64)
			}

			valueB, _ := b.Record[sortKey].Value.(float64)
			if options.UsePrev {
				valueB, _ = b.Record[sortKey].PrevValue.(float64)
			}

			if valueA > valueB {
				return -1
			} else if valueA < valueB {
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
	AddRank       bool
	PrevRanks     map[string]int
}

func (r SummaryRecordKeyPairs) Format(options FormatOptions) TableData {
	rows := [][]TableCell{}
	for j, record := range r.Entries {
		if options.Limit > 0 && j > options.Limit {
			break
		}

		row := []TableCell{}
		if options.AddRank {
			prev := 0
			if len(options.PrevRanks) > 0 {
				prev = options.PrevRanks[record.Key] + 1
			}

			row = append(row, TableCell{
				Value:        fmt.Sprintf("%d", j+1),
				RawValue:     j + 1,
				PrevRawValue: prev,
				Alignment:    TableColumnAlignmentRight,
			})
		}
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
	if options.AddRank {
		columns = append(columns, TableColumn{
			Name:      "#",
			Alignment: TableColumnAlignmentRight,
		})
	}
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
