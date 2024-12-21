package main

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"html/template"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/akamensky/argparse"
	"github.com/dustin/go-humanize"
	"github.com/myuon/akari/akari"
)

var (
	nginxLogRegexp      = regexp.MustCompile(`^(\S+) - (\S+) \[([^\]]+)\] "(\S+) (\S+) ([^"]+)" (\d+) (\d+) "([^"]+)" "([^"]+)" (\S+)$`)
	dbQueryLoggerRegexp = regexp.MustCompile(`^([0-9]{19})\s+([0-9]+)\s+(.*)$`)
	ulidLike            = regexp.MustCompile(`[0-9a-zA-Z]{26}`)
	uuidLike            = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
)

func parse(line string) []string {
	return nginxLogRegexp.FindStringSubmatch(line)
}

type NginxSummaryRecord struct {
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
	Key        NginxLogRecordKey
}

type NginxLogRecord struct {
	Status       int
	Bytes        int
	ResponseTime float64
	UserAgent    string
}

type NginxLogRecordKey struct {
	Protocol string
	Method   string
	Url      string
}

type LogRecordColumn struct {
	Name string
}

type LogRecords struct {
	Columns    []LogRecordColumn
	KeyColumns []LogRecordColumn
	Records    map[string][][]any
}

func (r LogRecords) GetIndex(key string) int {
	for i, column := range r.Columns {
		if column.Name == key {
			return i
		}
	}

	return -1
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

func parseLogRecords(r io.Reader) LogRecords {
	scanner := bufio.NewScanner(r)

	md5Hash := md5.New()
	records := map[string][][]any{}
	for scanner.Scan() {
		line := scanner.Text()

		tokens := parse(line)

		method := tokens[4]
		url := tokens[5]
		protocol := tokens[6]
		status, err := strconv.Atoi(tokens[7])
		if err != nil {
			log.Fatal(err)
		}
		bytes, err := strconv.Atoi(tokens[8])
		if err != nil {
			log.Fatal(err)
		}
		userAgent := tokens[10]
		responseTime, err := strconv.ParseFloat(tokens[11], 64)
		if err != nil {
			log.Fatal(err)
		}

		url = ulidLike.ReplaceAllLiteralString(url, "(ulid)")
		url = uuidLike.ReplaceAllLiteralString(url, "(uuid)")

		if strings.Contains(url, "?") {
			splitted := strings.Split(url, "?")
			path := splitted[0]

			masked := []string{}
			kvs := strings.Split(splitted[1], "&")
			for _, kv := range kvs {
				masked = append(masked, fmt.Sprintf("%s=*", strings.Split(kv, "=")[0]))
			}

			url = fmt.Sprintf("%s?%s", path, strings.Join(masked, "&"))
		}

		key := NginxLogRecordKey{
			Protocol: protocol,
			Method:   method,
			Url:      url,
		}

		hashKey := md5Hash.Sum([]byte(fmt.Sprintf("%v", key)))
		records[string(hashKey)] = append(records[string(hashKey)], []any{
			status,
			bytes,
			responseTime,
			userAgent,
			protocol,
			method,
			url,
		})
	}

	return LogRecords{
		Columns: []LogRecordColumn{
			{Name: "Status"},
			{Name: "Bytes"},
			{Name: "ResponseTime"},
			{Name: "UserAgent"},
			{Name: "Protocol"},
			{Name: "Method"},
			{Name: "Url"},
		},
		KeyColumns: []LogRecordColumn{
			{Name: "Protocol"},
			{Name: "Method"},
			{Name: "Url"},
		},
		Records: records,
	}
}

func analyzeSummary(logRecords LogRecords) []NginxSummaryRecord {
	summary := []NginxSummaryRecord{}
	for _, records := range logRecords.Records {
		requestTimes := []float64{}
		for _, record := range records {
			responseTime := record[logRecords.GetIndex("ResponseTime")].(float64)
			requestTimes = append(requestTimes, responseTime)
		}

		totalRequestTime := getSum(requestTimes)

		status2xx := 0
		status3xx := 0
		status4xx := 0
		status5xx := 0
		for _, record := range records {
			status := record[logRecords.GetIndex("Status")].(int)
			switch {
			case status >= 200 && status < 300:
				status2xx++
			case status >= 300 && status < 400:
				status3xx++
			case status >= 400 && status < 500:
				status4xx++
			case status >= 500 && status < 600:
				status5xx++
			}
		}

		bytesSlice := []int{}
		for _, record := range records {
			bytes := record[logRecords.GetIndex("Bytes")].(int)
			bytesSlice = append(bytesSlice, bytes)
		}

		if totalRequestTime < 0.001 {
			continue
		}

		summary = append(summary, NginxSummaryRecord{
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
			Key: NginxLogRecordKey{
				Protocol: records[0][logRecords.GetIndex("Protocol")].(string),
				Method:   records[0][logRecords.GetIndex("Method")].(string),
				Url:      records[0][logRecords.GetIndex("Url")].(string),
			},
		})
	}

	return summary
}

func analyzeNginxLog(r io.Reader, prev io.Reader, w io.Writer) {
	summary := analyzeSummary(parseLogRecords(r))

	prevSummary := map[NginxLogRecordKey]NginxSummaryRecord{}
	if prev != nil {
		sm := analyzeSummary(parseLogRecords(prev))

		for _, record := range sm {
			prevSummary[record.Key] = record
		}
	}

	slices.SortStableFunc(summary, func(a, b NginxSummaryRecord) int {
		if a.Total > b.Total {
			return -1
		} else if a.Total < b.Total {
			return 1
		} else {
			return strings.Compare(a.Key.Method, b.Key.Method)
		}
	})

	rows := [][]string{}

	for j, record := range summary {
		if j > 100 {
			break
		}

		prevRecord, ok := prevSummary[record.Key]

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

		rows = append(rows, []string{
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
			record.Key.Protocol,
			record.Key.Method,
			record.Key.Url,
		})
	}

	data := akari.TableData{
		Columns: []akari.TableColumn{
			{
				Name:      "Count",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "(diff)",
				Alignment: akari.TableColumnAlignmentLeft,
			},
			{
				Name:      "Total",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "(diff)",
				Alignment: akari.TableColumnAlignmentLeft,
			},
			{
				Name:      "Mean",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "(diff)",
				Alignment: akari.TableColumnAlignmentLeft,
			},
			{
				Name:      "Stddev",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "Min",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "P50",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "P90",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "P95",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "P99",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "Max",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "2xx",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "3xx",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "4xx",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "5xx",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "TotalBs",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "MinBs",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "MeanBs",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "MaxBs",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "Protocol",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "Method",
				Alignment: akari.TableColumnAlignmentLeft,
			},
			{
				Name:      "Url",
				Alignment: akari.TableColumnAlignmentLeft,
			},
		},
		Rows: rows,
	}
	data.WriteInText(w)
}

type DbLogRecord struct {
	Timestamp time.Time
	Elapsed   float64
	Query     string
}

func parseDbLogRecords(r io.Reader) map[string][]DbLogRecord {
	scanner := bufio.NewScanner(r)

	logRecords := map[string][]DbLogRecord{}
	for scanner.Scan() {
		line := scanner.Text()

		tokens := dbQueryLoggerRegexp.FindStringSubmatch(line)

		nanoSec, err := strconv.ParseInt(tokens[1], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		timestamp := time.Unix(nanoSec/1e9, nanoSec%1e9).Local()

		elapsedInNano, err := strconv.Atoi(tokens[2])
		if err != nil {
			log.Fatal(err)
		}
		elapsed := float64(elapsedInNano) / 1e6

		query := tokens[3]

		logRecords[query] = append(logRecords[query], DbLogRecord{
			Timestamp: timestamp,
			Elapsed:   elapsed,
			Query:     query,
		})
	}

	return logRecords
}

type DbSummaryRecord struct {
	Count int
	Total float64
	Query string
}

func analyzeDbSummary(logRecords map[string][]DbLogRecord) []DbSummaryRecord {
	summary := []DbSummaryRecord{}
	for query, records := range logRecords {
		elapsedTimes := []float64{}
		for _, record := range records {
			elapsedTimes = append(elapsedTimes, record.Elapsed)
		}

		totalElapsed := getSum(elapsedTimes)

		summary = append(summary, DbSummaryRecord{
			Count: len(records),
			Total: totalElapsed,
			Query: query,
		})
	}

	return summary
}

func analyzeDbQueryLog(r io.Reader, w io.Writer) {
	summary := analyzeDbSummary(parseDbLogRecords(r))

	slices.SortStableFunc(summary, func(a, b DbSummaryRecord) int {
		if a.Total > b.Total {
			return -1
		} else if a.Total < b.Total {
			return 1
		} else {
			return strings.Compare(a.Query, b.Query)
		}
	})

	rows := [][]string{}
	for j, record := range summary {
		if j > 100 {
			break
		}

		rows = append(rows, []string{
			strconv.Itoa(record.Count),
			fmt.Sprintf("%.3f", record.Total),
			record.Query,
		})
	}

	data := akari.TableData{
		Columns: []akari.TableColumn{
			{
				Name:      "Count",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "Total",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "Query",
				Alignment: akari.TableColumnAlignmentLeft,
			},
		},
		Rows: rows,
	}
	data.WriteInText(w)
}

var (
	templateFiles = template.Must(template.ParseGlob("templates/*.html"))
	rootDir       = "."
)

type FileData struct {
	Name       string
	Path       string
	IsDir      bool
	ModifiedAt time.Time
	Size       int64
	Peek       []byte
	LogType    string
	PrevPath   string
}

func (d FileData) SizeHuman() string {
	return humanize.Bytes(uint64(d.Size))
}

func (d FileData) PeekString() string {
	peek := string(d.Peek)
	if len(peek) > 100 {
		peek = fmt.Sprintf("%v...", peek[:100])
	}

	return peek
}

func (d FileData) ModifiedAtString() string {
	return d.ModifiedAt.Format(time.DateTime)
}

type PageData struct {
	Title string
	Files map[string][]FileData
}

func listFiles(root string) ([]FileData, error) {
	var files []FileData
	if err := filepath.WalkDir(root, func(path string, info os.DirEntry, _ error) error {
		if info.IsDir() {
			return nil
		}

		fileInfo, err := info.Info()
		if err != nil {
			return err
		}

		modifiedAt := fileInfo.ModTime()
		size := fileInfo.Size()

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		line := make([]byte, 512)
		n, err := file.Read(line)
		if err != nil && err != io.EOF {
			return err
		}
		line = line[:n]

		if strings.Contains(string(line), "\n") {
			line = []byte(strings.SplitN(string(line), "\n", 2)[0])
		}

		logType := "unknown"
		if nginxLogRegexp.Match(line) {
			logType = "nginx"
		} else if dbQueryLoggerRegexp.Match(line) {
			logType = "dbquery"
		}

		files = append(files, FileData{
			Name:       info.Name(),
			Path:       path,
			IsDir:      info.IsDir(),
			Size:       size,
			ModifiedAt: modifiedAt,
			Peek:       line,
			LogType:    logType,
		})

		return nil
	}); err != nil {
		return nil, err
	}

	return files, nil
}

func fileHandler(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("dir")
	if dir == "" {
		dir = rootDir
	}

	files, err := listFiles(dir)
	if err != nil {
		http.Error(w, "Failed to list files", http.StatusInternalServerError)
		log.Println("Error listing files:", err)
		return
	}

	filesByType := map[string][]FileData{}
	for _, file := range files {
		filesByType[file.LogType] = append(filesByType[file.LogType], file)
	}

	for _, files := range filesByType {
		slices.SortFunc(files, func(a, b FileData) int {
			if !a.ModifiedAt.Equal(b.ModifiedAt) {
				return b.ModifiedAt.Compare(a.ModifiedAt)
			} else {
				return strings.Compare(b.Name, a.Name)
			}
		})
	}

	for _, files := range filesByType {
		for i := range files {
			if i == len(files)-1 {
				continue
			}

			files[i].PrevPath = files[i+1].Path
		}
	}

	pageData := PageData{
		Title: "File List",
		Files: filesByType,
	}
	err = templateFiles.ExecuteTemplate(w, "files.html", pageData)
	if err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		log.Println("Template execution error:", err)
		return
	}
}

func rawFileHandler(w http.ResponseWriter, r *http.Request) {
	filePath := r.URL.Query().Get("file")
	if filePath == "" {
		http.Error(w, "File not specified", http.StatusBadRequest)
		return
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		http.Error(w, "Failed to read file", http.StatusInternalServerError)
		log.Println("Error reading file:", err)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if _, err := w.Write(content); err != nil {
		slog.Error("Failed to write response", "error", err)
	}
}

func viewFileHandler(w http.ResponseWriter, r *http.Request) {
	logType := r.URL.Query().Get("type")
	if logType == "" {
		logType = "nginx"
	}

	filePath := r.URL.Query().Get("file")
	if filePath == "" {
		http.Error(w, "File not specified", http.StatusBadRequest)
		return
	}

	logFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}

	prevFilePath := r.URL.Query().Get("prev")

	prevLogFile, err := os.Open(prevFilePath)
	if err != nil {
		prevLogFile = nil
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	if logType == "nginx" {
		analyzeNginxLog(logFile, prevLogFile, w)
	} else if logType == "dbquery" {
		analyzeDbQueryLog(logFile, w)
	} else {
		http.Error(w, "Unknown log type", http.StatusBadRequest)
	}
}

func main() {
	parser := argparse.NewParser("akari", "Log analyzer")

	// prev := parser.String("p", "prev", &argparse.Options{Required: false, Help: "Previous log file"})
	serveCommand := parser.NewCommand("serve", "Starts a web server to serve the log analyzer")
	logDir := serveCommand.StringPositional(nil)

	err := parser.Parse(os.Args)
	if err != nil {
		fmt.Print(parser.Usage(err))
	}

	if serveCommand != nil {
		rootDir = *logDir

		http.Handle("/public/", http.StripPrefix("/public/", http.FileServer(http.Dir("./public"))))
		http.HandleFunc("/", fileHandler)
		http.HandleFunc("/raw", rawFileHandler)
		http.HandleFunc("/view", viewFileHandler)

		hostName := "localhost"
		if val, ok := os.LookupEnv("HOSTNAME"); ok {
			hostName = val
		}

		port := 8089
		if val, ok := os.LookupEnv("PORT"); ok {
			port, _ = strconv.Atoi(val)
		}

		slog.Info("Starting server", "port", port, "url", fmt.Sprintf("http://localhost:%v", port))

		if err := http.ListenAndServe(fmt.Sprintf("%v:%v", hostName, port), nil); err != nil {
			slog.Error("Failed to start server", "error", err)
		}
	}
}
