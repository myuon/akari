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

func (c TableCell) Diff() float64 {
	if c.PrevRawValue == nil {
		return 0
	}

	switch c.RawValue.(type) {
	case int:
		v := c.RawValue.(int)
		p := c.PrevRawValue.(int)
		if p == 0 {
			return 0
		}

		return float64(v-p) / float64(p)
	case float64:
		v := c.RawValue.(float64)
		p := c.PrevRawValue.(float64)
		if p < 0.1 {
			return 0
		}

		return (v - p) / p
	default:
		return 0
	}
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

type HtmlOptions struct {
	DiffHeaders []string
}

func (o HtmlOptions) IsDiffHeader(header string) bool {
	for _, h := range o.DiffHeaders {
		if h == header {
			return true
		}
	}
	return false
}

func (d TableData) Html(options HtmlOptions) HtmlTableData {
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

		if options.IsDiffHeader(column.Name) {
			attrs := map[string]string{}
			attrs["data-diff"] = "true"

			headers = append(headers, HtmlTableHeader{
				Text:       "(diff)",
				Attributes: attrs,
				Style:      style,
			})
		}
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

			if options.IsDiffHeader(d.Columns[i].Name) {
				value := cell.Diff()

				attrs := map[string]string{}
				attrs["data-value"] = fmt.Sprintf("%v", value)

				htmlRow = append(htmlRow, HtmlTableCell{
					Text:       template.HTML(fmt.Sprintf("(%+d%%)", int(value*100))),
					Attributes: attrs,
				})
			}
		}
		rows = append(rows, htmlRow)
	}

	return HtmlTableData{
		Headers: headers,
		Rows:    rows,
	}
}
