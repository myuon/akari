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
)

var (
	nginxLogRegexp = regexp.MustCompile(`^(\S+) - (\S+) \[([^\]]+)\] "([^"]+)" (\d+) (\d+) "([^"]+)" "([^"]+)" (\S+)`)
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

func analyzeNginxLog(r io.Reader) {
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

		logRecords[request] = append(logRecords[request], LogRecord{
			Status:       status,
			Bytes:        bytes,
			ResponseTime: responseTime,
		})
	}

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

	slices.SortStableFunc(summary, func(a, b SummaryRecord) int {
		return int(b.Total - a.Total)
	})

	table := [][]string{}
	table = append(table, []string{
		"Count",
		"Total",
		"Mean",
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

	for _, record := range summary {
		table = append(table, []string{
			strconv.Itoa(record.Count),
			fmt.Sprintf("%.3f", record.Total),
			fmt.Sprintf("%.4f", record.Mean),
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
			strconv.Itoa(record.TotalBytes),
			strconv.Itoa(record.MinBytes),
			strconv.Itoa(record.MeanBytes),
			strconv.Itoa(record.MaxBytes),
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
			if i == 18 {
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
	args := os.Args
	if len(args) < 2 {
		fmt.Println("Usage: akari <file>")
		os.Exit(0)
	}

	file := args[1]
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	analyzeNginxLog(f)
}
