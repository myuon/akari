package akari

import (
	"fmt"
	"hash/maphash"
	"io"
)

type AnalyzeOptions struct {
	Config  AnalyzerConfig
	Source  io.Reader
	HasPrev bool
	Prev    io.Reader
	Logger  DebugLogger
	Seed    maphash.Seed
}

func Analyze(options AnalyzeOptions) (TableData, error) {
	parseOptions, err := options.Config.ParseOptions(options.Seed)
	if err != nil {
		return TableData{}, fmt.Errorf("Failed to prepare options (%w)", err)
	}

	queryOptions, err := options.Config.QueryOptions()
	if err != nil {
		return TableData{}, fmt.Errorf("Failed to prepare query options (%w)", err)
	}

	formatOptions, err := options.Config.FormatOptions()
	if err != nil {
		return TableData{}, fmt.Errorf("Failed to prepare format options (%w)", err)
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
