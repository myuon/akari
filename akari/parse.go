package akari

import (
	"bufio"
	"crypto/md5"
	"fmt"
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

	md5Hash := md5.New()
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
	for scanner.Scan() {
		line := scanner.Text()

		tokens := options.RegExp.FindStringSubmatch(line)

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

		hashKey := md5Hash.Sum([]byte(fmt.Sprintf("%v", key)))
		records[string(hashKey)] = append(records[string(hashKey)], row)
	}

	logger.Debug("Scan finished")

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
