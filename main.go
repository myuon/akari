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

func parseLogRecords(r io.Reader) akari.LogRecords {
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

		key := []any{
			protocol,
			method,
			url,
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

	return akari.LogRecords{
		Columns: []akari.LogRecordColumn{
			{Name: "Status"},
			{Name: "Bytes"},
			{Name: "ResponseTime"},
			{Name: "UserAgent"},
			{Name: "Protocol"},
			{Name: "Method"},
			{Name: "Url"},
		},
		KeyColumns: []akari.LogRecordColumn{
			{Name: "Protocol"},
			{Name: "Method"},
			{Name: "Url"},
		},
		Records: records,
	}
}

func analyzeSummary(logRecords akari.LogRecords) akari.SummaryRecords {
	summary := map[string][]any{}
	for key, records := range logRecords.Records {
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

		summary[key] = []any{
			len(records),
			totalRequestTime,
			totalRequestTime / float64(len(records)),
			getStddev(requestTimes),
			slices.Min(requestTimes),
			getPercentile(requestTimes, 50),
			getPercentile(requestTimes, 90),
			getPercentile(requestTimes, 95),
			getPercentile(requestTimes, 99),
			slices.Max(requestTimes),
			status2xx,
			status3xx,
			status4xx,
			status5xx,
			getSum(bytesSlice),
			slices.Min(bytesSlice),
			getMean(bytesSlice),
			slices.Max(bytesSlice),
			records[0][logRecords.GetIndex("Protocol")].(string),
			records[0][logRecords.GetIndex("Method")].(string),
			records[0][logRecords.GetIndex("Url")].(string),
		}
	}

	return akari.SummaryRecords{
		Columns: []akari.SummaryRecordColumn{
			{Name: "Count"},
			{Name: "Total"},
			{Name: "Mean"},
			{Name: "Stddev"},
			{Name: "Min"},
			{Name: "P50"},
			{Name: "P90"},
			{Name: "P95"},
			{Name: "P99"},
			{Name: "Max"},
			{Name: "2xx"},
			{Name: "3xx"},
			{Name: "4xx"},
			{Name: "5xx"},
			{Name: "TotalBytes"},
			{Name: "MinBytes"},
			{Name: "MeanBytes"},
			{Name: "MaxBytes"},
			{Name: "Protocol"},
			{Name: "Method"},
			{Name: "Url"},
		},
		Rows: summary,
	}
}

func analyzeNginxLog(r io.Reader, prev io.Reader, w io.Writer) {
	summary := analyzeSummary(parseLogRecords(r))

	prevSummary := akari.SummaryRecords{}
	if prev != nil {
		sm := analyzeSummary(parseLogRecords(prev))

		prevSummary = sm
	}

	summaryRecords := summary.GetKeyPairs()

	slices.SortStableFunc(summaryRecords, func(a, b akari.SummaryRecordKeyPair) int {
		totalIndex := summary.GetIndex("Total")
		if a.Record[totalIndex].(float64) > b.Record[totalIndex].(float64) {
			return -1
		} else if a.Record[totalIndex].(float64) < b.Record[totalIndex].(float64) {
			return 1
		} else {
			return strings.Compare(a.Key, b.Key)
		}
	})

	rows := [][]string{}

	for j, record := range summaryRecords {
		if j > 100 {
			break
		}

		prevRecord, ok := prevSummary.Rows[record.Key]

		countDiff := ""
		if ok {
			countIndex := summary.GetIndex("Count")
			countDiff = fmt.Sprintf("(%+d%%)", (record.Record[countIndex].(int)-prevRecord[countIndex].(int))*100/prevRecord[countIndex].(int))
		}

		totalDiff := ""
		if ok {
			totalIndex := summary.GetIndex("Total")
			totalDiff = fmt.Sprintf("(%+d%%)", int((record.Record[totalIndex].(float64)-prevRecord[totalIndex].(float64))*100/prevRecord[totalIndex].(float64)))
		}

		meanDiff := ""
		if ok {
			meanIndex := summary.GetIndex("Mean")
			meanDiff = fmt.Sprintf("(%+d%%)", int((record.Record[meanIndex].(float64)-prevRecord[meanIndex].(float64))*100/prevRecord[meanIndex].(float64)))
		}

		rows = append(rows, []string{
			strconv.Itoa(record.Record[summary.GetIndex("Count")].(int)),
			countDiff,
			fmt.Sprintf("%.3f", record.Record[summary.GetIndex("Total")].(float64)),
			totalDiff,
			fmt.Sprintf("%.4f", record.Record[summary.GetIndex("Mean")].(float64)),
			meanDiff,
			fmt.Sprintf("%.4f", record.Record[summary.GetIndex("Stddev")].(float64)),
			fmt.Sprintf("%.3f", record.Record[summary.GetIndex("Min")].(float64)),
			fmt.Sprintf("%.3f", record.Record[summary.GetIndex("P50")].(float64)),
			fmt.Sprintf("%.3f", record.Record[summary.GetIndex("P90")].(float64)),
			fmt.Sprintf("%.3f", record.Record[summary.GetIndex("P95")].(float64)),
			fmt.Sprintf("%.3f", record.Record[summary.GetIndex("P99")].(float64)),
			fmt.Sprintf("%.3f", record.Record[summary.GetIndex("Max")].(float64)),
			strconv.Itoa(record.Record[summary.GetIndex("2xx")].(int)),
			strconv.Itoa(record.Record[summary.GetIndex("3xx")].(int)),
			strconv.Itoa(record.Record[summary.GetIndex("4xx")].(int)),
			strconv.Itoa(record.Record[summary.GetIndex("5xx")].(int)),
			humanize.Bytes(uint64(record.Record[summary.GetIndex("TotalBytes")].(int))),
			humanize.Bytes(uint64(record.Record[summary.GetIndex("MinBytes")].(int))),
			humanize.Bytes(uint64(record.Record[summary.GetIndex("MeanBytes")].(int))),
			humanize.Bytes(uint64(record.Record[summary.GetIndex("MaxBytes")].(int))),
			record.Record[summary.GetIndex("Protocol")].(string),
			record.Record[summary.GetIndex("Method")].(string),
			record.Record[summary.GetIndex("Url")].(string),
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

func parseDbLogRecords(r io.Reader) akari.LogRecords {
	scanner := bufio.NewScanner(r)

	logRecords := map[string][][]any{}
	md5Hash := md5.New()
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

		key := []any{
			query,
		}
		hashKey := string(md5Hash.Sum([]byte(fmt.Sprintf("%v", key))))

		logRecords[hashKey] = append(logRecords[hashKey], []any{
			timestamp,
			elapsed,
			query,
		})
	}

	return akari.LogRecords{
		Columns: []akari.LogRecordColumn{
			{Name: "Timestamp"},
			{Name: "Elapsed"},
			{Name: "Query"},
		},
		KeyColumns: []akari.LogRecordColumn{
			{Name: "Query"},
		},
		Records: logRecords,
	}
}

func analyzeDbSummary(logRecords akari.LogRecords) akari.SummaryRecords {
	summary := map[string][]any{}
	for key, records := range logRecords.Records {
		elapsedTimes := []float64{}
		for _, record := range records {
			elapsedTimes = append(elapsedTimes, record[logRecords.GetIndex("Elapsed")].(float64))
		}

		totalElapsed := getSum(elapsedTimes)

		summary[key] = []any{
			len(records),
			totalElapsed,
			records[0][logRecords.GetIndex("Query")].(string),
		}
	}

	return akari.SummaryRecords{
		Columns: []akari.SummaryRecordColumn{
			{Name: "Count"},
			{Name: "Total"},
			{Name: "Query"},
		},
		Rows: summary,
	}
}

func analyzeDbQueryLog(r io.Reader, w io.Writer) {
	summary := analyzeDbSummary(parseDbLogRecords(r))

	records := summary.GetKeyPairs()

	slices.SortStableFunc(records, func(a, b akari.SummaryRecordKeyPair) int {
		totalIndex := summary.GetIndex("Total")
		if a.Record[totalIndex].(float64) > b.Record[totalIndex].(float64) {
			return -1
		} else if a.Record[totalIndex].(float64) < b.Record[totalIndex].(float64) {
			return 1
		} else {
			return strings.Compare(a.Key, b.Key)
		}
	})

	rows := [][]string{}
	for j, record := range records {
		if j > 100 {
			break
		}

		rows = append(rows, []string{
			strconv.Itoa(record.Record[summary.GetIndex("Count")].(int)),
			fmt.Sprintf("%.3f", record.Record[summary.GetIndex("Total")].(float64)),
			record.Record[summary.GetIndex("Query")].(string),
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
