package akari

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"regexp"

	"github.com/pierrec/xxHash/xxHash64"
)

type ParseColumnOptions struct {
	Name        string
	SubexpName  string
	SubexpIndex int
	Converters  []Converter
}

type ParseOptions struct {
	RegExp   *regexp.Regexp
	Columns  []ParseColumnOptions
	Keys     []string
	HashSeed uint64
}

func Parse(options ParseOptions, r io.Reader, logger DebugLogger) (LogRecords, error) {
	scanner := bufio.NewScanner(r)

	hash := xxHash64.New(options.HashSeed)
	records := map[string]LogRecordRows{}

	logger.Debug("Start scanning")

	subexpNames := options.RegExp.SubexpNames()
	subexpIndexByName := map[string]int{}
	for i, name := range subexpNames {
		if name != "" {
			subexpIndexByName[name] = i
		}
	}

	logger.Debug("Subexp names", "names", subexpNames)

	resultTypes := map[string]LogRecordType{}

	tokensLines := [][]string{}
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		tokensLines = append(tokensLines, options.RegExp.FindStringSubmatch(line))
	}

	logger.Debug("Scan finished", "lines", len(tokensLines))

	for _, tokens := range tokensLines {
		row := []any{}
		key := []any{}

		for _, column := range options.Columns {
			index := column.SubexpIndex
			if column.SubexpName != "" {
				index = subexpIndexByName[column.SubexpName]
			}
			value := tokens[index]

			valueAny := any(value)

			// Default type is string
			resultTypes[column.Name] = LogRecordTypeString
			for _, converter := range column.Converters {
				v, t, err := converter.Convert(valueAny)
				if err != nil {
					return LogRecords{}, fmt.Errorf("Failed to convert %v (%w)", valueAny, err)
				}

				valueAny, resultTypes[column.Name] = v, t
			}

			for _, columnKey := range options.Keys {
				if columnKey == column.Name {
					key = append(key, valueAny)
				}
			}

			row = append(row, valueAny)
		}

		hashKey := base64.RawStdEncoding.EncodeToString(hash.Sum([]byte(fmt.Sprintf("%v", key))))
		records[string(hashKey)] = append(records[string(hashKey)], row)
	}

	logger.Debug("Processing tokens finished")

	columns := []LogRecordColumn{}
	for _, column := range options.Columns {
		columns = append(columns, LogRecordColumn{
			Name: column.Name,
			Type: resultTypes[column.Name],
		})
	}

	return LogRecords{
		Columns: columns,
		Records: records,
	}, nil
}
