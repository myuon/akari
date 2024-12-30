package akari

import (
	"hash/maphash"
	"io"
	"log"
)

func Analyze(c AnalyzerConfig, r io.Reader, hasPrev bool, prev io.Reader, logger DebugLogger) TableData {
	parseOptions := ParseOption{
		RegExp:   c.Parser.RegExp,
		Columns:  c.Parser.Columns.Load(),
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
			f := query.Filter.Load()
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
					f := column.Filter.Load()
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

	parsed := Parse(parseOptions, r, logger)

	logger.Debug("Parsed")

	prevRows := map[string]LogRecordRows{}
	if hasPrev {
		p := Parse(parseOptions, prev, logger)

		prevRows = p.Records
	}

	// summarize
	summary, err := parsed.Summarize(queryOptions, prevRows)
	if err != nil {
		log.Fatalf("Failed to summarize: %v", err)
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

	return result
}
