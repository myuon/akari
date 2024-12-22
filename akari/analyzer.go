package akari

import (
	"io"
	"regexp"
)

type ParserColumnRegExpSpecifier struct {
	Name  string
	Index int
}

type ParserColumnConverterConfig struct {
	Type    string
	Options map[string]any
}

func (c ParserColumnConverterConfig) Load() Converter {
	switch c.Type {
	case "ParseInt64":
		return ConvertParseInt64{}
	case "UnixNano":
		return ConvertUnixNano{}
	case "Div":
		return ConvertDiv{Divisor: c.Options["Divisor"].(float64)}
	case "MysqlBulkClause":
		return ConvertMysqlBulkClause{}
	default:
		return nil
	}
}

type ParserColumnConfig struct {
	Name       string
	Specifier  ParserColumnRegExpSpecifier
	Converters []ParserColumnConverterConfig
}

func (c ParserColumnConfig) Load() ParseColumnOption {
	cs := []Converter{}
	for _, converter := range c.Converters {
		cs = append(cs, converter.Load())
	}

	return ParseColumnOption{
		Name:        c.Name,
		SubexpName:  c.Specifier.Name,
		SubexpIndex: c.Specifier.Index,
		Converters:  cs,
	}
}

type ParserColumnConfigs []ParserColumnConfig

func (c ParserColumnConfigs) Load() []ParseColumnOption {
	cs := []ParseColumnOption{}
	for _, column := range c {
		cs = append(cs, column.Load())
	}

	return cs
}

type ParserConfig struct {
	RegExp  *regexp.Regexp
	Columns ParserColumnConfigs
}

type QueryFormatConfig struct {
	Alignment string
	Format    string
}

type QueryConfig struct {
	Name         string
	From         string
	ValueType    QueryValueType
	Function     QueryFunction
	Filter       *QueryFilter
	FormatOption QueryFormatConfig
}

type AnalyzerConfig struct {
	Parser       ParserConfig
	GroupingKeys []string
	Query        []QueryConfig
	OrderKeys    []string
	Limit        int
}

func (c AnalyzerConfig) Analyze(r io.Reader, w io.Writer) {
	parseOptions := ParseOption{
		RegExp:  c.Parser.RegExp,
		Columns: c.Parser.Columns.Load(),
		Keys:    c.GroupingKeys,
	}
	queryOptions := []Query{}
	for _, query := range c.Query {
		queryOptions = append(queryOptions, Query{
			Name:      query.Name,
			From:      query.From,
			ValueType: query.ValueType,
			Function:  query.Function,
			Filter:    query.Filter,
		})
	}
	formatOptions := FormatOptions{
		Limit: c.Limit,
	}
	for _, query := range c.Query {
		formatOptions.ColumnOptions = append(formatOptions.ColumnOptions, FormatColumnOptions{
			Name:      query.Name,
			Format:    query.FormatOption.Format,
			Alignment: query.FormatOption.Alignment,
		})
	}

	summary := Parse(parseOptions, r).Summarize(queryOptions)

	records := summary.GetKeyPairs()

	orderKeyIndexes := []int{}
	for _, orderKey := range c.OrderKeys {
		orderKeyIndexes = append(orderKeyIndexes, summary.GetIndex(orderKey))
	}
	records.SortBy(orderKeyIndexes)

	data := records.Format(formatOptions)
	data.WriteInText(w)
}
