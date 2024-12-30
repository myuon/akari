package akari

import (
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
	spName := c.Specifier.Name
	if spName == "" && c.Specifier.Index == 0 {
		spName = c.Name
	}

	cs := []Converter{}
	for _, converter := range c.Converters {
		cs = append(cs, converter.Load())
	}

	return ParseColumnOption{
		Name:        c.Name,
		SubexpName:  spName,
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
	Columns      []QueryConfig
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
	Diffs        []string
	ShowRank     bool
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
