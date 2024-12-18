package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"slices"
	"strconv"

	"github.com/akamensky/argparse"
	"github.com/dustin/go-humanize"
)

var (
	nginxLogRegexp = regexp.MustCompile(`^(\S+) - (\S+) \[([^\]]+)\] "([^"]+)" (\d+) (\d+) "([^"]+)" "([^"]+)" (\S+)$`)
	ulidLike       = regexp.MustCompile(`[0-9A-Z]{26}`)
)

func parse(line string) []string {
	return nginxLogRegexp.FindStringSubmatch(line)
}

type SummaryRecord struct {
	Count      int
	Total      float64
	Mean       float64
	Stddev     float64
	Min        float64
	P50        float64
	P90        float64
	P95        float64
	P99        float64
	Max        float64
	Status2xx  int
	Status3xx  int
	Status4xx  int
	Status5xx  int
	TotalBytes int
	MinBytes   int
	MeanBytes  int
	MaxBytes   int
	Request    string
}

type LogRecord struct {
	Status       int
	Bytes        int
	ResponseTime float64
}

func getSum[T int | float64](values []T) T {
	total := 0.0
	for _, value := range values {
		total += float64(value)
	}
	return T(total)
}

func getMean[T int | float64](values []T) T {
	total := 0.0
	for _, value := range values {
		total += float64(value)
	}
	return T(total / float64(len(values)))
}

func getStddev(values []float64) float64 {
	mean := getMean(values)
	total := 0.0
	for _, value := range values {
		total += (value - mean) * (value - mean)
	}
	return total / float64(len(values))
}

func getPercentile(values_ []float64, percentile int) float64 {
	values := append([]float64{}, values_...)

	slices.Sort(values)

	index := (percentile * len(values)) / 100
	return values[index]
}

func parseLogRecords(r io.Reader) map[string][]LogRecord {
	scanner := bufio.NewScanner(r)

	logRecords := map[string][]LogRecord{}
	for scanner.Scan() {
		line := scanner.Text()

		tokens := parse(line)

		request := tokens[4]
		status, err := strconv.Atoi(tokens[5])
		if err != nil {
			log.Fatal(err)
		}
		bytes, err := strconv.Atoi(tokens[6])
		if err != nil {
			log.Fatal(err)
		}
		responseTime, err := strconv.ParseFloat(tokens[9], 64)
		if err != nil {
			log.Fatal(err)
		}

		request = ulidLike.ReplaceAllLiteralString(request, "[ulid]")

		logRecords[request] = append(logRecords[request], LogRecord{
			Status:       status,
			Bytes:        bytes,
			ResponseTime: responseTime,
		})
	}

	return logRecords
}

func analyzeSummary(logRecords map[string][]LogRecord) []SummaryRecord {
	summary := []SummaryRecord{}
	for path, records := range logRecords {
		requestTimes := []float64{}
		for _, record := range records {
			requestTimes = append(requestTimes, record.ResponseTime)
		}

		totalRequestTime := getSum(requestTimes)

		status2xx := 0
		status3xx := 0
		status4xx := 0
		status5xx := 0
		for _, record := range records {
			switch {
			case record.Status >= 200 && record.Status < 300:
				status2xx++
			case record.Status >= 300 && record.Status < 400:
				status3xx++
			case record.Status >= 400 && record.Status < 500:
				status4xx++
			case record.Status >= 500 && record.Status < 600:
				status5xx++
			}
		}

		bytesSlice := []int{}
		for _, record := range records {
			bytesSlice = append(bytesSlice, record.Bytes)
		}

		summary = append(summary, SummaryRecord{
			Count:      len(records),
			Total:      totalRequestTime,
			Mean:       totalRequestTime / float64(len(records)),
			Stddev:     getStddev(requestTimes),
			Min:        slices.Min(requestTimes),
			P50:        getPercentile(requestTimes, 50),
			P90:        getPercentile(requestTimes, 90),
			P95:        getPercentile(requestTimes, 95),
			P99:        getPercentile(requestTimes, 99),
			Max:        slices.Max(requestTimes),
			Status2xx:  status2xx,
			Status3xx:  status3xx,
			Status4xx:  status4xx,
			Status5xx:  status5xx,
			TotalBytes: getSum(bytesSlice),
			MinBytes:   slices.Min(bytesSlice),
			MeanBytes:  getMean(bytesSlice),
			MaxBytes:   slices.Max(bytesSlice),
			Request:    path,
		})
	}

	return summary
}

func analyzeNginxLog(r io.Reader, prev io.Reader) {
	summary := analyzeSummary(parseLogRecords(r))

	prevSummary := map[string]SummaryRecord{}
	if prev != nil {
		sm := analyzeSummary(parseLogRecords(prev))

		for _, record := range sm {
			prevSummary[record.Request] = record
		}
	}

	slices.SortStableFunc(summary, func(a, b SummaryRecord) int {
		return int(b.Total - a.Total)
	})

	table := [][]string{}
	table = append(table, []string{
		"Count",
		"(diff)",
		"Total",
		"(diff)",
		"Mean",
		"(diff)",
		"Stddev",
		"Min",
		"P50",
		"P90",
		"P95",
		"P99",
		"Max",
		"2xx",
		"3xx",
		"4xx",
		"5xx",
		"TotalBs",
		"MinBs",
		"MeanBs",
		"MaxBs",
		"Request",
	})

	for j, record := range summary {
		if j > 100 {
			break
		}

		prevRecord, ok := prevSummary[record.Request]

		countDiff := ""
		if ok {
			countDiff = fmt.Sprintf("(%+d%%)", (record.Count-prevRecord.Count)*100/prevRecord.Count)
		}

		totalDiff := ""
		if ok {
			totalDiff = fmt.Sprintf("(%+d%%)", int((record.Total-prevRecord.Total)*100/prevRecord.Total))
		}

		meanDiff := ""
		if ok {
			meanDiff = fmt.Sprintf("(%+d%%)", int((record.Mean-prevRecord.Mean)*100/prevRecord.Mean))
		}

		table = append(table, []string{
			strconv.Itoa(record.Count),
			countDiff,
			fmt.Sprintf("%.3f", record.Total),
			totalDiff,
			fmt.Sprintf("%.4f", record.Mean),
			meanDiff,
			fmt.Sprintf("%.4f", record.Stddev),
			fmt.Sprintf("%.3f", record.Min),
			fmt.Sprintf("%.3f", record.P50),
			fmt.Sprintf("%.3f", record.P90),
			fmt.Sprintf("%.3f", record.P95),
			fmt.Sprintf("%.3f", record.P99),
			fmt.Sprintf("%.3f", record.Max),
			strconv.Itoa(record.Status2xx),
			strconv.Itoa(record.Status3xx),
			strconv.Itoa(record.Status4xx),
			strconv.Itoa(record.Status5xx),
			humanize.Bytes(uint64(record.TotalBytes)),
			humanize.Bytes(uint64(record.MinBytes)),
			humanize.Bytes(uint64(record.MeanBytes)),
			humanize.Bytes(uint64(record.MaxBytes)),
			record.Request,
		})
	}

	widths := []int{}
	for _, row := range table {
		for i, cell := range row {
			if i >= len(widths) {
				widths = append(widths, 0)
			}
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	for _, row := range table {
		for i, cell := range row {
			if i == 1 || i == 3 || i == 5 || i == 21 {
				fmt.Printf("%-*s", widths[i], cell)
			} else {
				fmt.Printf("%*s", widths[i], cell)
			}
			if i < len(row)-1 {
				fmt.Print("  ")
			}
		}
		fmt.Println()
	}
}

func main() {
	parser := argparse.NewParser("akari", "Log analyzer")

	prev := parser.String("p", "prev", &argparse.Options{Required: false, Help: "Previous log file"})
	file := parser.StringPositional(nil)

	err := parser.Parse(os.Args)
	if err != nil {
		fmt.Print(parser.Usage(err))
	}

	var prevFile *os.File
	if prev != nil {
		p, err := os.Open(*prev)
		if err != nil {
			log.Fatal(err)
		}

		prevFile = p
	}

	logFile, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}

	analyzeNginxLog(logFile, prevFile)
}
