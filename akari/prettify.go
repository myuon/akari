package akari

import (
	"fmt"
	"io"
)

const (
	TableColumnAlignmentLeft  = "left"
	TableColumnAlignmentRight = "right"
)

type TableColumn struct {
	Index     int
	Name      string
	Alignment string
}

type TableData struct {
	Columns []TableColumn
	Rows    [][]string
}

func (d TableData) WriteInText(w io.Writer) {
	table := [][]string{}

	headers := []string{}
	widths := []int{}
	for _, column := range d.Columns {
		headers = append(headers, column.Name)
		widths = append(widths, len(column.Name))
	}

	table = append(table, headers)

	for _, row := range d.Rows {
		tableRow := []string{}
		for _, column := range d.Columns {
			tableRow = append(tableRow, row[column.Index])
			widths[column.Index] = max(widths[column.Index], len(row[column.Index]))
		}

		table = append(table, tableRow)
	}

	rightAligned := map[int]bool{}
	for _, column := range d.Columns {
		if column.Alignment == TableColumnAlignmentRight {
			rightAligned[column.Index] = true
		}
	}

	for _, row := range table {
		for i, cell := range row {
			if val, ok := rightAligned[i]; ok && val {
				fmt.Fprintf(w, "%*s", widths[i], cell)
			} else {
				fmt.Fprintf(w, "%-*s", widths[i], cell)
			}
			if i < len(row)-1 {
				fmt.Fprint(w, "  ")
			}
		}
		fmt.Fprintln(w)
	}
}
