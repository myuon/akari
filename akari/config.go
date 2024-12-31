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

func (c ParserColumnConfig) Load() (ParseColumnOptions, error) {
	spName := c.Specifier.Name
	if spName == "" && c.Specifier.Index == 0 {
		spName = c.Name
	}

	cs := []Converter{}
	for _, converter := range c.Converters {
		c, err := converter.Load()
		if err != nil {
			return ParseColumnOptions{}, fmt.Errorf("Failed to load converter (%w)", err)
		}

		cs = append(cs, c)
	}

	return ParseColumnOptions{
		Name:        c.Name,
		SubexpName:  spName,
		SubexpIndex: c.Specifier.Index,
		Converters:  cs,
	}, nil
}

type ParserColumnConfigs []ParserColumnConfig

func (c ParserColumnConfigs) Load() ([]ParseColumnOptions, error) {
	cs := []ParseColumnOptions{}
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

func (config AnalyzerConfig) ParseOptions(seed uint64) (ParseOptions, error) {
	columns, err := config.Parser.Columns.Load()
	if err != nil {
		return ParseOptions{}, fmt.Errorf("Failed to load columns (%w)", err)
	}

	parseOptions := ParseOptions{
		RegExp:   config.Parser.RegExp,
		Columns:  columns,
		Keys:     config.GroupingKeys,
		HashSeed: seed,
	}
	return parseOptions, nil
}

func (config AnalyzerConfig) QueryOptions() ([]Query, error) {
	queryOptions := []Query{}
	for _, query := range config.Query {
		var filter *QueryFilter
		if query.Filter != nil {
			f, err := query.Filter.Load()
			if err != nil {
				return nil, fmt.Errorf("Failed to load filter (%w)", err)
			}

			filter = &f
		}

		function := query.Function
		if function == "" {
			function = QueryFunctionAny
		}

		queryOption := Query{
			Name:     query.GetName(),
			From:     query.From,
			Function: function,
			Filter:   filter,
		}

		if len(query.Columns) > 0 {
			for _, column := range query.Columns {
				name := queryOption.Name
				if column.Name != nil {
					name = *column.Name
				}
				from := queryOption.From
				if column.From != "" {
					from = column.From
				}
				function := queryOption.Function
				if column.Function != "" {
					function = column.Function
				}
				filter := queryOption.Filter
				if column.Filter != nil {
					f, err := column.Filter.Load()
					if err != nil {
						return nil, fmt.Errorf("Failed to load filter (%w)", err)
					}

					filter = &f
				}

				queryOptions = append(queryOptions, Query{
					Name:     name,
					From:     from,
					Function: function,
					Filter:   filter,
				})
			}
		} else {
			queryOptions = append(queryOptions, queryOption)
		}
	}

	return queryOptions, nil
}

func (config AnalyzerConfig) FormatOptions() (FormatOptions, error) {
	columns := []FormatColumnOptions{}
	for _, query := range config.Query {
		if len(query.Columns) > 0 {
			for _, column := range query.Columns {
				name := query.GetName()
				if column.Name != nil {
					name = *column.Name
				}

				columns = append(columns, FormatColumnOptions{
					Name:          name,
					Format:        query.FormatOption.Format,
					Alignment:     query.FormatOption.Alignment,
					HumanizeBytes: query.FormatOption.HumanizeBytes,
				})
			}
		} else {
			columns = append(columns, FormatColumnOptions{
				Name:          query.GetName(),
				Format:        query.FormatOption.Format,
				Alignment:     query.FormatOption.Alignment,
				HumanizeBytes: query.FormatOption.HumanizeBytes,
			})
		}
	}

	return FormatOptions{
		ColumnOptions: columns,
		Limit:         config.Limit,
	}, nil
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
