package akari

import (
	"fmt"
	"hash/maphash"
	"io"
)

func PrepareOptions(config AnalyzerConfig, seed maphash.Seed) (ParseOption, []Query, FormatOptions, error) {
	columns, err := config.Parser.Columns.Load()
	if err != nil {
		return ParseOption{}, nil, FormatOptions{}, fmt.Errorf("Failed to load columns (%w)", err)
	}

	parseOptions := ParseOption{
		RegExp:   config.Parser.RegExp,
		Columns:  columns,
		Keys:     config.GroupingKeys,
		HashSeed: seed,
	}
	queryOptions := []Query{}
	formatOptions := FormatOptions{
		Limit: config.Limit,
	}
	for _, query := range config.Query {
		var filter *QueryFilter
		if query.Filter != nil {
			f, err := query.Filter.Load()
			if err != nil {
				return ParseOption{}, nil, FormatOptions{}, fmt.Errorf("Failed to load filter (%w)", err)
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
						return ParseOption{}, nil, FormatOptions{}, fmt.Errorf("Failed to load filter (%w)", err)
					}

					filter = &f
				}

				queryOptions = append(queryOptions, Query{
					Name:     name,
					From:     from,
					Function: function,
					Filter:   filter,
				})
				formatOptions.ColumnOptions = append(formatOptions.ColumnOptions, FormatColumnOptions{
					Name:          name,
					Format:        query.FormatOption.Format,
					Alignment:     query.FormatOption.Alignment,
					HumanizeBytes: query.FormatOption.HumanizeBytes,
				})
			}
		} else {
			queryOptions = append(queryOptions, queryOption)
			formatOptions.ColumnOptions = append(formatOptions.ColumnOptions, FormatColumnOptions{
				Name:          queryOption.Name,
				Format:        query.FormatOption.Format,
				Alignment:     query.FormatOption.Alignment,
				HumanizeBytes: query.FormatOption.HumanizeBytes,
			})
		}
	}

	return parseOptions, queryOptions, formatOptions, nil
}

type AnalyzeOptions struct {
	Config  AnalyzerConfig
	Source  io.Reader
	HasPrev bool
	Prev    io.Reader
	Logger  DebugLogger
	Seed    maphash.Seed
}

func Summarize(options AnalyzeOptions) (SummaryRecords, error) {
	parseOptions, queryOptions, _, err := PrepareOptions(options.Config, options.Seed)
	if err != nil {
		return SummaryRecords{}, fmt.Errorf("Failed to prepare options (%w)", err)
	}

	options.Logger.Debug("Loaded options")

	parsed, err := Parse(parseOptions, options.Source, options.Logger)
	if err != nil {
		return SummaryRecords{}, fmt.Errorf("Failed to parse (%w)", err)
	}

	options.Logger.Debug("Parsed")

	prevRows := map[string]LogRecordRows{}
	if options.HasPrev {
		p, err := Parse(parseOptions, options.Prev, options.Logger)
		if err != nil {
			return SummaryRecords{}, fmt.Errorf("Failed to parse previous (%w)", err)
		}

		prevRows = p.Records
	}

	// summarize
	summary, err := parsed.Summarize(queryOptions, prevRows)
	if err != nil {
		return SummaryRecords{}, fmt.Errorf("Failed to summarize (%w)", err)
	}

	options.Logger.Debug("Summarized")

	return summary, nil
}

func Analyze(options AnalyzeOptions) (TableData, error) {
	parseOptions, queryOptions, formatOptions, err := PrepareOptions(options.Config, options.Seed)
	if err != nil {
		return TableData{}, fmt.Errorf("Failed to prepare options (%w)", err)
	}

	options.Logger.Debug("Loaded options")

	parsed, err := Parse(parseOptions, options.Source, options.Logger)
	if err != nil {
		return TableData{}, fmt.Errorf("Failed to parse (%w)", err)
	}

	options.Logger.Debug("Parsed")

	prevRows := map[string]LogRecordRows{}
	if options.HasPrev {
		p, err := Parse(parseOptions, options.Prev, options.Logger)
		if err != nil {
			return TableData{}, fmt.Errorf("Failed to parse previous (%w)", err)
		}

		prevRows = p.Records
	}

	// summarize
	summary, err := parsed.Summarize(queryOptions, prevRows)
	if err != nil {
		return TableData{}, fmt.Errorf("Failed to summarize (%w)", err)
	}

	options.Logger.Debug("Summarized")

	records := summary.GetKeyPairs()

	orderKeyIndexes := []int{}
	for _, orderKey := range options.Config.SortKeys {
		orderKeyIndexes = append(orderKeyIndexes, summary.GetIndex(orderKey))
	}

	// sort
	prevRanks := map[string]int{}
	if options.Config.ShowRank && options.HasPrev {
		records.SortBy(SortByOptions{
			SortKeyIndexes: orderKeyIndexes,
			UsePrev:        true,
		})

		for i, record := range records.Entries {
			prevRanks[record.Key] = i
		}
	}

	records.SortBy(SortByOptions{
		SortKeyIndexes: orderKeyIndexes,
	})

	options.Logger.Debug("Sorted")

	// format
	formatOptions.AddRank = options.Config.ShowRank
	formatOptions.PrevRanks = prevRanks
	result := records.Format(formatOptions)

	options.Logger.Debug("Formatted")

	return result, nil
}
