package akari

import (
	"bufio"
	"fmt"
	"hash/maphash"
	"io"
	"regexp"
)

type ParseColumnOption struct {
	Name        string
	SubexpName  string
	SubexpIndex int
	Converters  []Converter
}

type ParseOption struct {
	RegExp  *regexp.Regexp
	Columns []ParseColumnOption
	Keys    []string
}

func Parse(options ParseOption, r io.Reader, logger DebugLogger) LogRecords {
	scanner := bufio.NewScanner(r)

	hash := maphash.Hash{}
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
				valueAny = converter.Convert(valueAny)
				resultTypes[column.Name] = converter.ResultType()
			}

			for _, columnKey := range options.Keys {
				if columnKey == column.Name {
					key = append(key, valueAny)
				}
			}

			row = append(row, valueAny)
		}

		hash.Reset()
		hash.WriteString(fmt.Sprintf("%v", key))
		hashKey := hash.Sum(nil)
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
	}
}
