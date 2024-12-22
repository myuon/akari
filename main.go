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

	"github.com/BurntSushi/toml"
	"github.com/akamensky/argparse"
	"github.com/dustin/go-humanize"
	"github.com/myuon/akari/akari"
)

var (
	nginxLogRegexp      = regexp.MustCompile(`^(\S+) - (\S+) \[([^\]]+)\] "(?P<Method>\S+) (?P<Url>\S+) (?P<Protocol>[^"]+)" (?P<Status>\d+) (?P<Bytes>\d+) "([^"]+)" "(?P<UserAgent>[^"]+)" (?P<ResponseTime>\S+)$`)
	dbQueryLoggerRegexp = regexp.MustCompile(`^([0-9]{19})\s+([0-9]+)\s+(.*)$`)
)

func analyzeNginxLog(r io.Reader, prev io.Reader, w io.Writer) {
	parseOptions := akari.ParseOption{
		RegExp: nginxLogRegexp,
		Columns: []akari.ParseColumnOption{
			{
				Name:       "Status",
				SubexpName: "Status",
				Converters: []akari.Converter{akari.ConvertParseInt{}},
			},
			{
				Name:       "Bytes",
				SubexpName: "Bytes",
				Converters: []akari.Converter{akari.ConvertParseInt{}},
			},
			{
				Name:       "ResponseTime",
				SubexpName: "ResponseTime",
				Converters: []akari.Converter{akari.ConvertParseFloat64{}},
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
				Converters: []akari.Converter{
					akari.ConvertUlid{Tag: "(ulid)"},
					akari.ConvertUuid{Tag: "(uuid)"},
					akari.ConvertQueryParams{Tag: "*"},
				},
			},
		},
		Keys: []string{"Protocol", "Method", "Url"},
	}
	query := []akari.Query{
		{
			Name:      "Count",
			Function:  akari.QueryFunctionCount,
			ValueType: akari.QueryValueTypeFloat64,
			From:      "ResponseTime",
		},
		{
			Name:      "Total",
			From:      "ResponseTime",
			ValueType: akari.QueryValueTypeFloat64,
			Function:  akari.QueryFunctionSum,
		},
		{
			Name:      "Mean",
			From:      "ResponseTime",
			ValueType: akari.QueryValueTypeFloat64,
			Function:  akari.QueryFunctionMean,
		},
		{
			Name:      "Stddev",
			From:      "ResponseTime",
			ValueType: akari.QueryValueTypeFloat64,
			Function:  akari.QueryFunctionStddev,
		},
		{
			Name:      "Min",
			From:      "ResponseTime",
			ValueType: akari.QueryValueTypeFloat64,
			Function:  akari.QueryFunctionMin,
		},
		{
			Name:      "P50",
			From:      "ResponseTime",
			ValueType: akari.QueryValueTypeFloat64,
			Function:  akari.QueryFunctionP50,
		},
		{
			Name:      "P90",
			From:      "ResponseTime",
			ValueType: akari.QueryValueTypeFloat64,
			Function:  akari.QueryFunctionP90,
		},
		{
			Name:      "P95",
			From:      "ResponseTime",
			ValueType: akari.QueryValueTypeFloat64,
			Function:  akari.QueryFunctionP95,
		},
		{
			Name:      "P99",
			From:      "ResponseTime",
			ValueType: akari.QueryValueTypeFloat64,
			Function:  akari.QueryFunctionP99,
		},
		{
			Name:      "Max",
			From:      "ResponseTime",
			Function:  akari.QueryFunctionMax,
			ValueType: akari.QueryValueTypeFloat64,
		},
		{
			Name:      "2xx",
			From:      "Status",
			ValueType: akari.QueryValueTypeInt,
			Function:  akari.QueryFunctionCount,
			Filter: &akari.QueryFilter{
				Type: akari.QueryFilterTypeBetween,
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
			ValueType: akari.QueryValueTypeInt,
			Function:  akari.QueryFunctionCount,
			Filter: &akari.QueryFilter{
				Type: akari.QueryFilterTypeBetween,
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
			ValueType: akari.QueryValueTypeInt,
			Function:  akari.QueryFunctionCount,
			Filter: &akari.QueryFilter{
				Type: akari.QueryFilterTypeBetween,
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
			ValueType: akari.QueryValueTypeInt,
			Function:  akari.QueryFunctionCount,
			Filter: &akari.QueryFilter{
				Type: akari.QueryFilterTypeBetween,
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
			ValueType: akari.QueryValueTypeInt,
			Function:  akari.QueryFunctionSum,
		},
		{
			Name:      "MinBytes",
			From:      "Bytes",
			ValueType: akari.QueryValueTypeInt,
			Function:  akari.QueryFunctionMin,
		},
		{
			Name:      "MeanBytes",
			From:      "Bytes",
			ValueType: akari.QueryValueTypeInt,
			Function:  akari.QueryFunctionMean,
		},
		{
			Name:      "MaxBytes",
			From:      "Bytes",
			ValueType: akari.QueryValueTypeInt,
			Function:  akari.QueryFunctionMax,
		},
		{
			Name:      "Protocol",
			From:      "Protocol",
			ValueType: akari.QueryValueTypeString,
			Function:  akari.QueryFunctionAny,
		},
		{
			Name:      "Method",
			Function:  akari.QueryFunctionAny,
			From:      "Method",
			ValueType: akari.QueryValueTypeString,
		},
		{
			Name:      "Url",
			Function:  akari.QueryFunctionAny,
			From:      "Url",
			ValueType: akari.QueryValueTypeString,
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
				Converters:  []akari.Converter{akari.ConvertParseInt64{}, akari.ConvertUnixNano{}},
			},
			{
				Name:        "Elapsed",
				SubexpIndex: 2,
				Converters:  []akari.Converter{akari.ConvertParseInt64{}, akari.ConvertDiv{Divisor: 1e9}},
			},
			{
				Name:        "Query",
				SubexpIndex: 3,
				Converters:  []akari.Converter{akari.ConvertMysqlBulkClause{}},
			},
		},
		Keys: []string{"Query"},
	}
	summary := akari.Parse(parseOptions, r).Summarize([]akari.Query{
		{
			Name:      "Count",
			From:      "Elapsed",
			Function:  akari.QueryFunctionCount,
			ValueType: akari.QueryValueTypeFloat64,
		},
		{
			Name:      "Total",
			From:      "Elapsed",
			ValueType: akari.QueryValueTypeFloat64,
			Function:  akari.QueryFunctionSum,
		},
		{
			Name:      "Query",
			From:      "Query",
			Function:  akari.QueryFunctionAny,
			ValueType: akari.QueryValueTypeString,
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
	templateFiles  = template.Must(template.ParseGlob("templates/*.html"))
	rootDir        = "."
	configFilePath = "akari.toml"
	config         = akari.AkariConfig{}
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
		for _, analyzer := range config.Analyzers {
			if analyzer.Parser.RegExp.Match(line) {
				logType = analyzer.Name
				break
			}
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

	for _, analyzer := range config.Analyzers {
		if logType == analyzer.Name {
			analyzer.Analyze(logFile, prevLogFile, w)
			return
		}
	}

	http.Error(w, "Unknown log type", http.StatusBadRequest)
}

func main() {
	parser := argparse.NewParser("akari", "Log analyzer")
	verbose := parser.Flag("v", "verbose", &argparse.Options{Help: "Verbose mode"})

	initCommand := parser.NewCommand("init", "Generates a new akari configuration file")
	serveCommand := parser.NewCommand("serve", "Starts a web server to serve the log analyzer")
	configFile := serveCommand.String("c", "akari.toml", &argparse.Options{Help: "Configuration file path"})
	logDir := serveCommand.StringPositional(nil)

	if err := parser.Parse(os.Args); err != nil {
		fmt.Print(parser.Usage(err))
	}

	if *verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	if initCommand.Happened() {
		file, err := os.Create("akari.toml")
		if err != nil {
			log.Fatal(err)
		}

		toml.NewEncoder(file).Encode(akari.AkariConfig{})
	} else if serveCommand.Happened() {
		rootDir = *logDir
		configFilePath = *configFile

		if _, err := toml.DecodeFile(configFilePath, &config); err != nil {
			log.Fatal(err)
		}

		slog.Debug("Loaded config", "path", configFile, "config", config)

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
