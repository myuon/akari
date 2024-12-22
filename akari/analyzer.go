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
	case "uuid":
		return ConvertUuid{Tag: c.Options["tag"].(string)}
	case "ulid":
		return ConvertUlid{Tag: c.Options["tag"].(string)}
	case "unixNano":
		return ConvertUnixNano{}
	case "unixMilli":
		return ConvertUnixMilli{}
	case "unix":
		return ConvertUnix{}
	case "div":
		return ConvertDiv{Divisor: c.Options["divisor"].(float64)}
	case "queryParams":
		return ConvertQueryParams{Tag: c.Options["tag"].(string)}
	case "mysqlBulkClause":
		return ConvertMysqlBulkClause{}
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
	Name         string
	From         string
	ValueType    QueryValueType
	Function     QueryFunction
	Filter       *QueryFilterConfig
	FormatOption QueryFormatConfig
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
}

func (c AnalyzerConfig) Analyze(r io.Reader, prev io.Reader, w io.Writer) {
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

		queryOptions = append(queryOptions, Query{
			Name:      query.Name,
			From:      query.From,
			ValueType: query.ValueType,
			Function:  query.Function,
			Filter:    filter,
		})
	}
	formatOptions := FormatOptions{
		Limit: c.Limit,
	}
	for _, query := range c.Query {
		formatOptions.ColumnOptions = append(formatOptions.ColumnOptions, FormatColumnOptions{
			Name:          query.Name,
			Format:        query.FormatOption.Format,
			Alignment:     query.FormatOption.Alignment,
			HumanizeBytes: query.FormatOption.HumanizeBytes,
		})
	}

	// parse, summarize
	summary := Parse(parseOptions, r).Summarize(queryOptions)

	prevSummary := SummaryRecords{}
	if prev != nil {
		prevSummary = Parse(parseOptions, prev).Summarize(queryOptions)
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
		formatOptions.ColumnOptions = append(formatOptions.ColumnOptions[:add.At], append([]FormatColumnOptions{option}, formatOptions.ColumnOptions[add.At:]...)...)
	}

	records := summary.GetKeyPairs()

	orderKeyIndexes := []int{}
	for _, orderKey := range c.SortKeys {
		orderKeyIndexes = append(orderKeyIndexes, summary.GetIndex(orderKey))
	}

	// sort
	records.SortBy(orderKeyIndexes)

	// format
	data := records.Format(formatOptions)

	// output
	data.WriteInText(w)
}

type AkariConfig struct {
	Analyzers []AnalyzerConfig
}
