package akari

import (
	"hash/maphash"
	"io"
	"log"
)

func Analyze(c AnalyzerConfig, r io.Reader, prev io.Reader, logger DebugLogger) TableData {
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
	if prev != nil {
		p := Parse(parseOptions, prev, logger)

		prevRows = p.Records
	}

	// summarize
	summary, err := parsed.Summarize(queryOptions, prevRows)
	if err != nil {
		log.Fatalf("Failed to summarize: %v", err)
	}

	logger.Debug("Summarized")

	// transform
	/*
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
		if c.ShowRank {
			summary.Insert(0, SummaryRecordColumn{Name: "Rank"}, func(key string, row []any) any {
				// NOTE: rankはsortした後にしか確定しないので、ここでは一旦何も返さない。あとで再計算したものを設定するようにする
				return 0
			})
			formatOptions.ColumnOptions = InsertAt(formatOptions.ColumnOptions, 0, FormatColumnOptions{
				Name:      "#",
				Format:    "%d",
				Alignment: TableColumnAlignmentRight,
			})
		}
	*/

	logger.Debug("Transformed")

	records := summary.GetKeyPairs()

	orderKeyIndexes := []int{}
	for _, orderKey := range c.SortKeys {
		orderKeyIndexes = append(orderKeyIndexes, summary.GetIndex(orderKey))
	}

	// sort
	records.SortBy(orderKeyIndexes)

	logger.Debug("Sorted")

	if c.ShowRank {
		for i, pair := range records.Entries {
			// Rankは0列目
			pair.Record[0].Value = i + 1
		}
	}

	logger.Debug("Add rank")

	// format
	result := records.Format(formatOptions)

	logger.Debug("Formatted")

	return result
}
