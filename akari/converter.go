package akari

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Converter interface {
	Convert(any) (any, LogRecordType, error)
}

type ConvertParseInt struct{}

func (c ConvertParseInt) Convert(a any) (any, LogRecordType, error) {
	i, err := strconv.Atoi(a.(string))
	if err != nil {
		return nil, "", fmt.Errorf("Failed to convert %v to int: %v", a, err)
	}

	return i, LogRecordTypeInt, nil
}

type ConvertParseInt64 struct{}

func (c ConvertParseInt64) Convert(a any) (any, LogRecordType, error) {
	i, err := strconv.ParseInt(a.(string), 10, 64)
	if err != nil {
		return nil, "", fmt.Errorf("Failed to convert %v to int64 (%w)", a, err)
	}

	return i, LogRecordTypeInt64, nil
}

type ConvertParseFloat64 struct{}

func (c ConvertParseFloat64) Convert(a any) (any, LogRecordType, error) {
	f, err := strconv.ParseFloat(a.(string), 64)
	if err != nil {
		return nil, "", fmt.Errorf("Failed to convert %v to float64 (%w)", a, err)
	}

	return f, LogRecordTypeFloat64, nil
}

type ConvertQueryParams struct {
	Replacer string
}

func (c ConvertQueryParams) Convert(a any) (any, LogRecordType, error) {
	url := a.(string)

	if strings.Contains(url, "?") {
		splitted := strings.Split(url, "?")
		path := splitted[0]

		masked := []string{}
		kvs := strings.Split(splitted[1], "&")
		for _, kv := range kvs {
			masked = append(masked, fmt.Sprintf("%s=%v", strings.Split(kv, "=")[0], c.Replacer))
		}

		url = fmt.Sprintf("%s?%s", path, strings.Join(masked, "&"))
	}

	return url, LogRecordTypeString, nil
}

type ConvertUnixNano struct{}

func (c ConvertUnixNano) Convert(a any) (any, LogRecordType, error) {
	nanoSec := a.(int64)

	timestamp := time.Unix(nanoSec/1e9, nanoSec%1e9).Local()
	return timestamp, LogRecordTypeDateTime, nil
}

type ConvertUnixMilli struct{}

func (c ConvertUnixMilli) Convert(a any) (any, LogRecordType, error) {
	milliSec := a.(int64)

	timestamp := time.Unix(milliSec/1e3, (milliSec%1e3)*1e6).Local()
	return timestamp, LogRecordTypeDateTime, nil
}

type ConvertUnix struct{}

func (c ConvertUnix) Convert(a any) (any, LogRecordType, error) {
	sec := a.(int64)

	timestamp := time.Unix(sec, 0).Local()
	return timestamp, LogRecordTypeDateTime, nil
}

type ConvertDiv struct {
	Divisor float64
}

func (c ConvertDiv) Convert(a any) (any, LogRecordType, error) {
	return float64(a.(int64)) / c.Divisor, LogRecordTypeFloat64, nil
}

type ConvertRegexpReplace struct {
	RegExp   *regexp.Regexp
	Replacer string
}

func (c ConvertRegexpReplace) Convert(a any) (any, LogRecordType, error) {
	return c.RegExp.ReplaceAllString(a.(string), c.Replacer), LogRecordTypeString, nil
}
