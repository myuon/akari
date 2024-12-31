package akari

import "html/template"

type HtmlTableHeader struct {
	Text       string
	Attributes map[string]string
	Style      map[string]string
}

type HtmlTableCell struct {
	Text       template.HTML
	Attributes map[string]string
	Style      map[string]string
}

type HtmlTableRow struct {
	Key   string
	Cells []HtmlTableCell
}

type HtmlTableData struct {
	Headers []HtmlTableHeader
	Rows    []HtmlTableRow
}
