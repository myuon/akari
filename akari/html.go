package akari

import (
	"fmt"
	"html/template"
	"strings"
)

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

func HtmlStyle(style map[string]string) template.CSS {
	result := []string{}
	for key, value := range style {
		result = append(result, fmt.Sprintf("%v:%v", key, value))
	}

	return template.CSS(strings.Join(result, ";"))
}

func HtmlAttrs(attrs map[string]string) template.HTMLAttr {
	result := []string{}
	for key, value := range attrs {
		result = append(result, fmt.Sprintf(`%v="%v"`, key, value))
	}

	return template.HTMLAttr(strings.Join(result, " "))
}
