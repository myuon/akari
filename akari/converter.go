package akari

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	ulidLike = regexp.MustCompile(`[0-9a-zA-Z]{26}`)
	uuidLike = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
)

type Converter interface {
	Convert(any) any
	ResultType() LogRecordType
}

type ConvertParseInt struct{}

func (c ConvertParseInt) Convert(a any) any {
	i, err := strconv.Atoi(a.(string))
	if err != nil {
		log.Fatalf("Failed to convert %v to int: %v", a, err)
	}

	return i
}

func (c ConvertParseInt) ResultType() LogRecordType {
	return LogRecordTypeInt
}

type ConvertParseInt64 struct{}

func (c ConvertParseInt64) Convert(a any) any {
	i, err := strconv.ParseInt(a.(string), 10, 64)
	if err != nil {
		log.Fatalf("Failed to convert %v to int64: %v", a, err)
	}

	return i
}

func (c ConvertParseInt64) ResultType() LogRecordType {
	return LogRecordTypeInt64
}

type ConvertParseFloat64 struct{}

func (c ConvertParseFloat64) Convert(a any) any {
	f, err := strconv.ParseFloat(a.(string), 64)
	if err != nil {
		log.Fatalf("Failed to convert %v to float64: %v", a, err)
	}

	return f
}

func (c ConvertParseFloat64) ResultType() LogRecordType {
	return LogRecordTypeFloat64
}

type ConvertUlid struct {
	Replacer string
}

func (c ConvertUlid) Convert(a any) any {
	return ulidLike.ReplaceAllLiteralString(a.(string), c.Replacer)
}

func (c ConvertUlid) ResultType() LogRecordType {
	return LogRecordTypeString
}

type ConvertUuid struct {
	Replacer string
}

func (c ConvertUuid) Convert(a any) any {
	return uuidLike.ReplaceAllLiteralString(a.(string), c.Replacer)
}

func (c ConvertUuid) ResultType() LogRecordType {
	return LogRecordTypeString
}

type ConvertQueryParams struct {
	Replacer string
}

func (c ConvertQueryParams) Convert(a any) any {
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

	return url
}

func (c ConvertQueryParams) ResultType() LogRecordType {
	return LogRecordTypeString
}

type ConvertUnixNano struct{}

func (c ConvertUnixNano) Convert(a any) any {
	nanoSec := a.(int64)

	timestamp := time.Unix(nanoSec/1e9, nanoSec%1e9).Local()
	return timestamp
}

func (c ConvertUnixNano) ResultType() LogRecordType {
	return LogRecordTypeDateTime
}

type ConvertUnixMilli struct{}

func (c ConvertUnixMilli) Convert(a any) any {
	milliSec := a.(int64)

	timestamp := time.Unix(milliSec/1e3, (milliSec%1e3)*1e6).Local()
	return timestamp
}

func (c ConvertUnixMilli) ResultType() LogRecordType {
	return LogRecordTypeDateTime
}

type ConvertUnix struct{}

func (c ConvertUnix) Convert(a any) any {
	sec := a.(int64)

	timestamp := time.Unix(sec, 0).Local()
	return timestamp
}

func (c ConvertUnix) ResultType() LogRecordType {
	return LogRecordTypeDateTime
}

type ConvertDiv struct {
	Divisor float64
}

func (c ConvertDiv) Convert(a any) any {
	return float64(a.(int64)) / c.Divisor
}

func (c ConvertDiv) ResultType() LogRecordType {
	return LogRecordTypeFloat64
}

type ConvertRegexpReplace struct {
	RegExp   *regexp.Regexp
	Replacer string
}

func (c ConvertRegexpReplace) Convert(a any) any {
	return c.RegExp.ReplaceAllString(a.(string), c.Replacer)
}

func (c ConvertRegexpReplace) ResultType() LogRecordType {
	return LogRecordTypeString
}
