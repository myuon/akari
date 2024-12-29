package akari

import (
	"io"
	"log"
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
	case "parseInt":
		return ConvertParseInt{}
	case "parseInt64":
		return ConvertParseInt64{}
	case "parseFloat64":
		return ConvertParseFloat64{}
	case "unixNano":
		return ConvertUnixNano{}
	case "unixMilli":
		return ConvertUnixMilli{}
	case "unix":
		return ConvertUnix{}
	case "div":
		return ConvertDiv{Divisor: c.Options["divisor"].(float64)}
	case "queryParams":
		return ConvertQueryParams{Replacer: c.Options["replacer"].(string)}
	case "regexp":
		return ConvertRegexpReplace{
			RegExp:   regexp.MustCompile(c.Options["pattern"].(string)),
			Replacer: c.Options["replacer"].(string),
		}
	default:
		log.Fatalf("Unknown converter type: %v", c.Type)
		return nil
	}
}

type ParserColumnConfig struct {
	Name       string
	Specifier  ParserColumnRegExpSpecifier
	Converters []ParserColumnConverterConfig
}

func (c ParserColumnConfig) Load() ParseColumnOption {
	name := c.Name
	if name == "" {
		name = c.Specifier.Name
	}

	cs := []Converter{}
	for _, converter := range c.Converters {
		cs = append(cs, converter.Load())
	}

	return ParseColumnOption{
		Name:        name,
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

type QueryFilterConfig struct {
	Type    string
	Options map[string]any
}

func (c QueryFilterConfig) Load() QueryFilter {
	switch c.Type {
	case "between":
		return QueryFilter{
			Type: QueryFilterTypeBetween,
			Between: struct {
				Start float64
				End   float64
			}{
				Start: float64(c.Options["start"].(int64)),
				End:   float64(c.Options["end"].(int64)),
			},
		}
	default:
		log.Fatalf("Unknown filter type: %v", c.Type)
		return QueryFilter{}
	}
}

type QueryFormatConfig struct {
	Alignment     string
	Format        string
	HumanizeBytes bool
}

type QueryConfig struct {
	Name         *string
	From         string
	Function     QueryFunction
	Filter       *QueryFilterConfig
	FormatOption QueryFormatConfig
}

func (c QueryConfig) GetName() string {
	if c.Name != nil {
		return *c.Name
	}

	return c.From
}

type InsertColumnConfigType string

const (
	InsertColumnConfigTypeDiff InsertColumnConfigType = "diff"
)

type AddColumnConfig struct {
	Name         string
	At           int
	Type         InsertColumnConfigType
	From         string
	FormatOption QueryFormatConfig
}

type AnalyzerConfig struct {
	Name         string
	Parser       ParserConfig
	GroupingKeys []string
	Query        []QueryConfig
	SortKeys     []string
	Limit        int
	AddColumn    []AddColumnConfig
	Diffs        []string // shorthand for AddColumn
}

func (c AnalyzerConfig) Analyze(r io.Reader, prev io.Reader) TableData {
	parseOptions := ParseOption{
		RegExp:  c.Parser.RegExp,
		Columns: c.Parser.Columns.Load(),
		Keys:    c.GroupingKeys,
	}
	queryOptions := []Query{}
	for _, query := range c.Query {
		var filter *QueryFilter
		if query.Filter != nil {
			f := query.Filter.Load()
			filter = &f
		}

		function := query.Function
		if function == "" {
			function = QueryFunctionAny
		}

		queryOptions = append(queryOptions, Query{
			Name:     query.GetName(),
			From:     query.From,
			Function: function,
			Filter:   filter,
		})
	}
	formatOptions := FormatOptions{
		Limit: c.Limit,
	}
	for _, query := range c.Query {
		formatOptions.ColumnOptions = append(formatOptions.ColumnOptions, FormatColumnOptions{
			Name:          query.GetName(),
			Format:        query.FormatOption.Format,
			Alignment:     query.FormatOption.Alignment,
			HumanizeBytes: query.FormatOption.HumanizeBytes,
		})
	}

	// parse, summarize
	summary, err := Parse(parseOptions, r).Summarize(queryOptions)
	if err != nil {
		log.Fatalf("Failed to summarize: %v", err)
	}

	prevSummary := SummaryRecords{}
	if prev != nil {
		prevSummary, err = Parse(parseOptions, prev).Summarize(queryOptions)
		if err != nil {
			log.Fatalf("Failed to summarize: %v", err)
		}
	}

	// transform
	for _, add := range c.AddColumn {
		summary.Insert(add.At, SummaryRecordColumn{Name: add.Name}, func(key string, row []any) any {
			prevRecord, ok := prevSummary.Rows[key]
			if ok {
				if current, ok := row[summary.GetIndex(add.From)].(int); ok {
					if prev, ok := prevRecord[prevSummary.GetIndex(add.From)].(int); ok {
						if current > 0 && prev > 0 {
							return (current - prev) * 100 / prev
						}
					}
				} else if current, ok := row[summary.GetIndex(add.From)].(float64); ok {
					if prev, ok := prevRecord[prevSummary.GetIndex(add.From)].(float64); ok {
						if current > 0 && prev > 0 {
							return int((current - prev) * 100 / prev)
						}
					}
				}
			}

			return 0
		})

		option := FormatColumnOptions{
			Name:          add.Name,
			Format:        add.FormatOption.Format,
			Alignment:     add.FormatOption.Alignment,
			HumanizeBytes: add.FormatOption.HumanizeBytes,
		}
		formatOptions.ColumnOptions = InsertAt(formatOptions.ColumnOptions, add.At, option)
	}
	for _, from := range c.Diffs {
		at := summary.GetIndex(from) + 1
		summary.Insert(at, SummaryRecordColumn{Name: "(diff)"}, func(key string, row []any) any {
			prevRecord, ok := prevSummary.Rows[key]
			if ok {
				if current, ok := row[summary.GetIndex(from)].(int); ok {
					if prev, ok := prevRecord[prevSummary.GetIndex(from)].(int); ok {
						if current > 0 && prev > 0 {
							return (current - prev) * 100 / prev
						}
					}
				} else if current, ok := row[summary.GetIndex(from)].(float64); ok {
					if prev, ok := prevRecord[prevSummary.GetIndex(from)].(float64); ok {
						if current > 0 && prev > 0 {
							return int((current - prev) * 100 / prev)
						}
					}
				}
			}

			return 0
		})

		option := FormatColumnOptions{
			Name:          "(diff)",
			Format:        "(%+d%%)",
			Alignment:     TableColumnAlignmentLeft,
			HumanizeBytes: false,
		}
		formatOptions.ColumnOptions = InsertAt(formatOptions.ColumnOptions, at, option)
	}

	records := summary.GetKeyPairs()

	orderKeyIndexes := []int{}
	for _, orderKey := range c.SortKeys {
		orderKeyIndexes = append(orderKeyIndexes, summary.GetIndex(orderKey))
	}

	// sort
	records.SortBy(orderKeyIndexes)

	// format
	return records.Format(formatOptions)
}

type AkariConfig struct {
	Analyzers []AnalyzerConfig
}

func (c AkariConfig) GetLogTypes() []string {
	logTypes := []string{}
	for _, analyzer := range c.Analyzers {
		logTypes = append(logTypes, analyzer.Name)
	}

	return logTypes
}
