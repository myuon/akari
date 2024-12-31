package akari

import (
	"fmt"
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

func (c ParserColumnConverterConfig) Load() (Converter, error) {
	switch c.Type {
	case "parseInt":
		return ConvertParseInt{}, nil
	case "parseInt64":
		return ConvertParseInt64{}, nil
	case "parseFloat64":
		return ConvertParseFloat64{}, nil
	case "unixNano":
		return ConvertUnixNano{}, nil
	case "unixMilli":
		return ConvertUnixMilli{}, nil
	case "unix":
		return ConvertUnix{}, nil
	case "div":
		return ConvertDiv{Divisor: c.Options["divisor"].(float64)}, nil
	case "queryParams":
		return ConvertQueryParams{Replacer: c.Options["replacer"].(string)}, nil
	case "regexp":
		return ConvertRegexpReplace{
			RegExp:   regexp.MustCompile(c.Options["pattern"].(string)),
			Replacer: c.Options["replacer"].(string),
		}, nil
	default:
		return nil, fmt.Errorf("Unknown converter type: %v", c.Type)
	}
}

type ParserColumnConfig struct {
	Name       string
	Specifier  ParserColumnRegExpSpecifier
	Converters []ParserColumnConverterConfig
}

func (c ParserColumnConfig) Load() (ParseColumnOption, error) {
	spName := c.Specifier.Name
	if spName == "" && c.Specifier.Index == 0 {
		spName = c.Name
	}

	cs := []Converter{}
	for _, converter := range c.Converters {
		c, err := converter.Load()
		if err != nil {
			return ParseColumnOption{}, fmt.Errorf("Failed to load converter (%w)", err)
		}

		cs = append(cs, c)
	}

	return ParseColumnOption{
		Name:        c.Name,
		SubexpName:  spName,
		SubexpIndex: c.Specifier.Index,
		Converters:  cs,
	}, nil
}

type ParserColumnConfigs []ParserColumnConfig

func (c ParserColumnConfigs) Load() ([]ParseColumnOption, error) {
	cs := []ParseColumnOption{}
	for _, column := range c {
		c, err := column.Load()
		if err != nil {
			return nil, fmt.Errorf("Failed to load column (%w)", err)
		}

		cs = append(cs, c)
	}

	return cs, nil
}

type ParserConfig struct {
	RegExp  *regexp.Regexp
	Columns ParserColumnConfigs
}

type QueryFilterConfig struct {
	Type    string
	Options map[string]any
}

func (c QueryFilterConfig) Load() (QueryFilter, error) {
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
		}, nil
	default:
		return QueryFilter{}, fmt.Errorf("Unknown filter type: %v", c.Type)
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
