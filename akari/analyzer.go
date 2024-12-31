package akari

import (
	"fmt"
	"hash/maphash"
	"io"
)

func Analyze(c AnalyzerConfig, r io.Reader, hasPrev bool, prev io.Reader, logger DebugLogger) (TableData, error) {
	columns, err := c.Parser.Columns.Load()
	if err != nil {
		return TableData{}, fmt.Errorf("Failed to load columns (%w)", err)
	}

	parseOptions := ParseOption{
		RegExp:   c.Parser.RegExp,
		Columns:  columns,
		Keys:     c.GroupingKeys,
		HashSeed: maphash.MakeSeed(),
	}
	queryOptions := []Query{}
	formatOptions := FormatOptions{
		Limit: c.Limit,
	}
	for _, query := range c.Query {
		var filter *QueryFilter
		if query.Filter != nil {
			f, err := query.Filter.Load()
			if err != nil {
				return TableData{}, fmt.Errorf("Failed to load filter (%w)", err)
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
						return TableData{}, fmt.Errorf("Failed to load filter (%w)", err)
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

	logger.Debug("Loaded options")

	parsed, err := Parse(parseOptions, r, logger)
	if err != nil {
		return TableData{}, fmt.Errorf("Failed to parse (%w)", err)
	}

	logger.Debug("Parsed")

	prevRows := map[string]LogRecordRows{}
	if hasPrev {
		p, err := Parse(parseOptions, prev, logger)
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

	logger.Debug("Summarized")

	records := summary.GetKeyPairs()

	orderKeyIndexes := []int{}
	for _, orderKey := range c.SortKeys {
		orderKeyIndexes = append(orderKeyIndexes, summary.GetIndex(orderKey))
	}

	// sort
	prevRanks := map[string]int{}
	if c.ShowRank && hasPrev {
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

	logger.Debug("Sorted")

	// format
	formatOptions.AddRank = c.ShowRank
	formatOptions.PrevRanks = prevRanks
	result := records.Format(formatOptions)

	logger.Debug("Formatted")

	return result, nil
}
