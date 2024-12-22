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
	Converter   func(string) any
	Replacer    func(any) any
}

type ParseOption struct {
	RegExp  *regexp.Regexp
	Columns []ParseColumnOption
	Keys    []string
}

func Parse(options ParseOption, r io.Reader) LogRecords {
	scanner := bufio.NewScanner(r)

	md5Hash := md5.New()
	records := map[string]LogRecordRows{}

	subexpNames := options.RegExp.SubexpNames()
	subexpIndexByName := map[string]int{}
	for i, name := range subexpNames {
		if name != "" {
			subexpIndexByName[name] = i
		}
	}

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

			convert := column.Converter
			replace := column.Replacer
			if column.Converter == nil {
				convert = func(s string) any { return s }
			}
			if column.Replacer == nil {
				replace = func(a any) any { return a }
			}

			valueAny := replace(convert(value))
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

	columns := []LogRecordColumn{}
	for _, column := range options.Columns {
		columns = append(columns, LogRecordColumn{Name: column.Name})
	}

	return LogRecords{
		Columns: columns,
		Records: records,
	}
}
