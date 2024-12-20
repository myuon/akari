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
	Name      string
	Alignment string
}

type TableData struct {
	Columns []TableColumn
	Rows    [][]string
}

func (d TableData) WriteInText(w io.Writer) {
	table := [][]string{}
	columnNameIndex := map[string]int{}
	for i, column := range d.Columns {
		columnNameIndex[column.Name] = i
	}

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
			i := columnNameIndex[column.Name]
			tableRow = append(tableRow, row[i])
			widths[i] = max(widths[i], len(row[i]))
		}

		table = append(table, tableRow)
	}

	rightAligned := map[int]bool{}
	for _, column := range d.Columns {
		i := columnNameIndex[column.Name]
		if column.Alignment == TableColumnAlignmentRight {
			rightAligned[i] = true
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
