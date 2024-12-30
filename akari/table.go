package akari

import (
	"fmt"
	"html/template"
	"io"
	"strings"
)

const (
	TableColumnAlignmentLeft  = "left"
	TableColumnAlignmentRight = "right"
)

type TableColumn struct {
	Name      string
	Alignment string
}

type TableCell struct {
	Value        string
	RawValue     any
	PrevRawValue any
	Alignment    string
}

type TableData struct {
	Columns []TableColumn
	Rows    [][]TableCell
}

func (d TableData) Write(w io.Writer) {
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
		for i := range d.Columns {
			tableRow = append(tableRow, row[i].Value)
			widths[i] = max(widths[i], len(row[i].Value))
		}

		table = append(table, tableRow)
	}

	rightAligned := map[int]bool{}
	for i, column := range d.Columns {
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

func (d TableData) Html() HtmlTableData {
	headers := []HtmlTableHeader{}
	for _, column := range d.Columns {
		style := map[string]string{}
		if column.Alignment != "" {
			style["text-align"] = column.Alignment
		}

		headers = append(headers, HtmlTableHeader{
			Text:  column.Name,
			Style: style,
		})
	}

	rows := [][]HtmlTableCell{}
	for _, row := range d.Rows {
		htmlRow := []HtmlTableCell{}
		for i := range d.Columns {
			cell := row[i]

			style := map[string]string{}
			if cell.Alignment != "" {
				style["text-align"] = cell.Alignment
			}

			attrs := map[string]string{}
			attrs["data-value"] = fmt.Sprintf("%v", cell.RawValue)
			if cell.PrevRawValue != nil {
				attrs["data-prev-value"] = fmt.Sprintf("%v", cell.PrevRawValue)
			}

			htmlRow = append(htmlRow, HtmlTableCell{
				Text:       template.HTML(strings.ReplaceAll(cell.Value, " ", "&nbsp;")),
				Attributes: attrs,
				Style:      style,
			})
		}
		rows = append(rows, htmlRow)
	}

	return HtmlTableData{
		Headers: headers,
		Rows:    rows,
	}
}
