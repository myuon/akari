package main

import (
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
	nginxLogRegexp      = regexp.MustCompile(`^(\S+) - (\S+) \[([^\]]+)\] "(?P<Method>\S+) (?P<Url>\S+) (?P<Protocol>[^"]+)" (?P<Status>\d+) (?P<Bytes>\d+) "([^"]+)" "(?P<UserAgent>[^"]+)" (?P<ResponseTime>\S+)$`)
	dbQueryLoggerRegexp = regexp.MustCompile(`^([0-9]{19})\s+([0-9]+)\s+(.*)$`)
	ulidLike            = regexp.MustCompile(`[0-9a-zA-Z]{26}`)
	uuidLike            = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
)

func analyzeNginxLog(r io.Reader, prev io.Reader, w io.Writer) {
	parseOptions := akari.ParseOption{
		RegExp: nginxLogRegexp,
		Columns: []akari.ParseColumnOption{
			{
				Name:       "Status",
				SubexpName: "Status",
				Converter: func(s string) any {
					v, err := strconv.Atoi(s)
					if err != nil {
						log.Fatal(err)
					}

					return v
				},
			},
			{
				Name:       "Bytes",
				SubexpName: "Bytes",
				Converter: func(s string) any {
					v, err := strconv.Atoi(s)
					if err != nil {
						log.Fatal(err)
					}

					return v
				},
			},
			{
				Name:       "ResponseTime",
				SubexpName: "ResponseTime",
				Converter: func(s string) any {
					v, err := strconv.ParseFloat(s, 64)
					if err != nil {
						log.Fatal(err)
					}

					return v
				},
			},
			{
				Name:       "UserAgent",
				SubexpName: "UserAgent",
			},
			{
				Name:       "Protocol",
				SubexpName: "Protocol",
			},
			{
				Name:       "Method",
				SubexpName: "Method",
			},
			{
				Name:       "Url",
				SubexpName: "Url",
				Replacer: func(a any) any {
					url := a.(string)
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

					return url
				},
			},
		},
		Keys: []akari.ParseColumnOption{
			{Name: "Protocol"},
			{Name: "Method"},
			{Name: "Url"},
		},
	}
	query := []akari.Aggregation{
		{
			Name:      "Count",
			Function:  akari.AggregationFunctionCount,
			ValueType: akari.AggregationValueTypeFloat64,
			From:      "ResponseTime",
		},
		{
			Name:      "Total",
			From:      "ResponseTime",
			ValueType: akari.AggregationValueTypeFloat64,
			Function:  akari.AggregationFunctionSum,
		},
		{
			Name:      "Mean",
			From:      "ResponseTime",
			ValueType: akari.AggregationValueTypeFloat64,
			Function:  akari.AggregationFunctionMean,
		},
		{
			Name:      "Stddev",
			From:      "ResponseTime",
			ValueType: akari.AggregationValueTypeFloat64,
			Function:  akari.AggregationFunctionStddev,
		},
		{
			Name:      "Min",
			From:      "ResponseTime",
			ValueType: akari.AggregationValueTypeFloat64,
			Function:  akari.AggregationFunctionMin,
		},
		{
			Name:      "P50",
			From:      "ResponseTime",
			ValueType: akari.AggregationValueTypeFloat64,
			Function:  akari.AggregationFunctionP50,
		},
		{
			Name:      "P90",
			From:      "ResponseTime",
			ValueType: akari.AggregationValueTypeFloat64,
			Function:  akari.AggregationFunctionP90,
		},
		{
			Name:      "P95",
			From:      "ResponseTime",
			ValueType: akari.AggregationValueTypeFloat64,
			Function:  akari.AggregationFunctionP95,
		},
		{
			Name:      "P99",
			From:      "ResponseTime",
			ValueType: akari.AggregationValueTypeFloat64,
			Function:  akari.AggregationFunctionP99,
		},
		{
			Name:      "Max",
			From:      "ResponseTime",
			Function:  akari.AggregationFunctionMax,
			ValueType: akari.AggregationValueTypeFloat64,
		},
		{
			Name:      "2xx",
			From:      "Status",
			ValueType: akari.AggregationValueTypeInt,
			Function:  akari.AggregationFunctionCount,
			Filter: &akari.AggregationFilter{
				Type: akari.AggregationFilterTypeBetween,
				Between: struct {
					Start float64
					End   float64
				}{
					Start: 200,
					End:   300,
				},
			},
		},
		{
			Name:      "3xx",
			From:      "Status",
			ValueType: akari.AggregationValueTypeInt,
			Function:  akari.AggregationFunctionCount,
			Filter: &akari.AggregationFilter{
				Type: akari.AggregationFilterTypeBetween,
				Between: struct {
					Start float64
					End   float64
				}{
					Start: 300,
					End:   400,
				},
			},
		},
		{
			Name:      "4xx",
			From:      "Status",
			ValueType: akari.AggregationValueTypeInt,
			Function:  akari.AggregationFunctionCount,
			Filter: &akari.AggregationFilter{
				Type: akari.AggregationFilterTypeBetween,
				Between: struct {
					Start float64
					End   float64
				}{
					Start: 400,
					End:   500,
				},
			},
		},
		{
			Name:      "5xx",
			From:      "Status",
			ValueType: akari.AggregationValueTypeInt,
			Function:  akari.AggregationFunctionCount,
			Filter: &akari.AggregationFilter{
				Type: akari.AggregationFilterTypeBetween,
				Between: struct {
					Start float64
					End   float64
				}{
					Start: 500,
					End:   600,
				},
			},
		},
		{
			Name:      "TotalBytes",
			From:      "Bytes",
			ValueType: akari.AggregationValueTypeInt,
			Function:  akari.AggregationFunctionSum,
		},
		{
			Name:      "MinBytes",
			From:      "Bytes",
			ValueType: akari.AggregationValueTypeInt,
			Function:  akari.AggregationFunctionMin,
		},
		{
			Name:      "MeanBytes",
			From:      "Bytes",
			ValueType: akari.AggregationValueTypeInt,
			Function:  akari.AggregationFunctionMean,
		},
		{
			Name:      "MaxBytes",
			From:      "Bytes",
			ValueType: akari.AggregationValueTypeInt,
			Function:  akari.AggregationFunctionMax,
		},
		{
			Name:      "Protocol",
			From:      "Protocol",
			ValueType: akari.AggregationValueTypeString,
			Function:  akari.AggregationFunctionAny,
		},
		{
			Name:      "Method",
			Function:  akari.AggregationFunctionAny,
			From:      "Method",
			ValueType: akari.AggregationValueTypeString,
		},
		{
			Name:      "Url",
			Function:  akari.AggregationFunctionAny,
			From:      "Url",
			ValueType: akari.AggregationValueTypeString,
		},
	}
	summary := akari.Parse(parseOptions, r).Summarize(query)

	prevSummary := akari.SummaryRecords{}
	if prev != nil {
		sm := akari.Parse(parseOptions, prev).Summarize(query)

		prevSummary = sm
	}

	summary.Insert(1, akari.SummaryRecordColumn{Name: "(diff)"}, func(key string, row []any) any {
		prevRecord, ok := prevSummary.Rows[key]
		if ok {
			current := row[summary.GetIndex("Count")].(int)
			prev := prevRecord[prevSummary.GetIndex("Count")].(int)

			return (current - prev) * 100 / prev
		}

		return 0
	})
	summary.Insert(3, akari.SummaryRecordColumn{Name: "(diff)"}, func(key string, row []any) any {
		prevRecord, ok := prevSummary.Rows[key]
		if ok {
			current := row[summary.GetIndex("Total")].(float64)
			prev := prevRecord[prevSummary.GetIndex("Total")].(float64)

			if prev > 0 && current > 0 {
				return int((current - prev) * 100 / prev)
			}
		}

		return 0
	})
	summary.Insert(5, akari.SummaryRecordColumn{Name: "(diff)"}, func(key string, row []any) any {
		prevRecord, ok := prevSummary.Rows[key]
		if ok {
			current := row[summary.GetIndex("Mean")].(float64)
			prev := prevRecord[prevSummary.GetIndex("Mean")].(float64)

			if prev > 0 && current > 0 {
				return int((current - prev) * 100 / prev)
			}
		}

		return 0
	})

	records := summary.GetKeyPairs()
	records.SortBy([]int{summary.GetIndex("Total")})

	data := records.Format(akari.FormatOptions{
		ColumnOptions: []akari.FormatColumnOptions{
			{
				Name:      "Count",
				Alignment: akari.TableColumnAlignmentRight,
			},
			{
				Name:      "(diff)",
				Alignment: akari.TableColumnAlignmentLeft,
				Format:    "(%+d%%)",
			},
			{
				Name:      "Total",
				Alignment: akari.TableColumnAlignmentRight,
				Format:    "%.3f",
			},
			{
				Name:      "(diff)",
				Alignment: akari.TableColumnAlignmentLeft,
				Format:    "(%+d%%)",
			},
			{
				Name:      "Mean",
				Alignment: akari.TableColumnAlignmentRight,
				Format:    "%.4f",
			},
			{
				Name:      "(diff)",
				Alignment: akari.TableColumnAlignmentLeft,
				Format:    "(%+d%%)",
			},
			{
				Name:      "Stddev",
				Alignment: akari.TableColumnAlignmentRight,
				Format:    "%.4f",
			},
			{
				Name:      "Min",
				Alignment: akari.TableColumnAlignmentRight,
				Format:    "%.3f",
			},
			{
				Name:      "P50",
				Alignment: akari.TableColumnAlignmentRight,
				Format:    "%.3f",
			},
			{
				Name:      "P90",
				Alignment: akari.TableColumnAlignmentRight,
				Format:    "%.3f",
			},
			{
				Name:      "P95",
				Alignment: akari.TableColumnAlignmentRight,
				Format:    "%.3f",
			},
			{
				Name:      "P99",
				Alignment: akari.TableColumnAlignmentRight,
				Format:    "%.3f",
			},
			{
				Name:      "Max",
				Alignment: akari.TableColumnAlignmentRight,
				Format:    "%.3f",
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
		Limit: 100,
	})
	data.WriteInText(w)
}

func analyzeDbQueryLog(r io.Reader, w io.Writer) {
	parseOptions := akari.ParseOption{
		RegExp: dbQueryLoggerRegexp,
		Columns: []akari.ParseColumnOption{
			{
				Name:        "Timestamp",
				SubexpIndex: 1,
				Converter: func(s string) any {
					nanoSec, err := strconv.ParseInt(s, 10, 64)
					if err != nil {
						log.Fatal(err)
					}
					timestamp := time.Unix(nanoSec/1e9, nanoSec%1e9).Local()

					return timestamp
				},
			},
			{
				Name:        "Elapsed",
				SubexpIndex: 2,
				Converter: func(s string) any {
					elapsedInNano, err := strconv.Atoi(s)
					if err != nil {
						log.Fatal(err)
					}
					elapsed := float64(elapsedInNano) / 1e9

					return elapsed
				},
			},
			{
				Name:        "Query",
				SubexpIndex: 3,
			},
		},
		Keys: []akari.ParseColumnOption{{Name: "Query"}},
	}
	summary := akari.Parse(parseOptions, r).Summarize([]akari.Aggregation{
		{
			Name:      "Count",
			From:      "Elapsed",
			Function:  akari.AggregationFunctionCount,
			ValueType: akari.AggregationValueTypeFloat64,
		},
		{
			Name:      "Total",
			From:      "Elapsed",
			ValueType: akari.AggregationValueTypeFloat64,
			Function:  akari.AggregationFunctionSum,
		},
		{
			Name:      "Query",
			From:      "Query",
			Function:  akari.AggregationFunctionAny,
			ValueType: akari.AggregationValueTypeString,
		},
	})

	records := summary.GetKeyPairs()
	records.SortBy([]int{summary.GetIndex("Total")})

	data := records.Format(akari.FormatOptions{
		ColumnOptions: []akari.FormatColumnOptions{
			{Name: "Count", Alignment: akari.TableColumnAlignmentRight},
			{Name: "Total", Format: "%.3f", Alignment: akari.TableColumnAlignmentRight},
			{Name: "Query", Alignment: akari.TableColumnAlignmentLeft},
		},
		Limit: 100,
	})
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
