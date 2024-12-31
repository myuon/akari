package cmd

import (
	"context"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/fsnotify/fsnotify"
	"github.com/myuon/akari/akari"
)

var (
	config = akari.NewGlobalVar(akari.AkariConfig{})
)

type FileData struct {
	Name       string
	Path       string
	DirPath    string
	IsDir      bool
	ModifiedAt time.Time
	Size       int64
	Peek       []byte
	LogType    string
	PrevPath   string
}

func (d FileData) SizeHuman() string {
	return akari.HumanizeBytes(int(d.Size))
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

type PageDataFile struct {
	DirPath string
	Content []FileData
}

type PageData struct {
	Title string
	Files []PageDataFile
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
		for _, analyzer := range config.Load().Analyzers {
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
			DirPath:    filepath.Dir(path),
		})

		return nil
	}); err != nil {
		return nil, err
	}

	return files, nil
}

func logGroupHandler(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("dir")
	if dir == "" {
		dir, _ = os.Getwd()
	}

	files, err := listFiles(dir)
	if err != nil {
		http.Error(w, "Failed to list files", http.StatusInternalServerError)
		log.Println("Error listing files:", err)
		return
	}

	slices.SortFunc(files, func(a, b FileData) int {
		if a.LogType != b.LogType {
			return strings.Compare(a.LogType, b.LogType)
		} else if a.DirPath != b.DirPath {
			return strings.Compare(a.DirPath, b.DirPath)
		} else if !a.ModifiedAt.Equal(b.ModifiedAt) {
			return b.ModifiedAt.Compare(a.ModifiedAt)
		} else {
			return strings.Compare(b.Name, a.Name)
		}
	})
	for i, file := range files {
		for j := i + 1; j < len(files); j++ {
			if files[j].LogType == file.LogType {
				files[j].PrevPath = file.Path
				break
			}
		}
	}

	filesByDirPath := map[string][]FileData{}
	for _, file := range files {
		filesByDirPath[file.DirPath] = append(filesByDirPath[file.DirPath], file)
	}

	entries := []PageDataFile{}
	for dirPath, files := range filesByDirPath {
		entries = append(entries, PageDataFile{
			DirPath: dirPath,
			Content: files,
		})
	}
	slices.SortStableFunc(entries, func(a, b PageDataFile) int {
		return strings.Compare(b.DirPath, a.DirPath)
	})

	serverData := UseServerData(r)

	pageData := PageData{
		Title: "Akari",
		Files: entries,
	}
	if err = serverData.TemplateFiles.ExecuteTemplate(w, "files.html", pageData); err != nil {
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
		http.Error(w, "Failed to open file", http.StatusInternalServerError)
		return
	}

	prevFilePath := r.URL.Query().Get("prev")

	hasPrev := true
	prevLogFile, err := os.Open(prevFilePath)
	if err != nil {
		slog.Warn("Failed to open previous file", "error", err)
		hasPrev = false
	}

	serverData := UseServerData(r)

	tableData := akari.HtmlTableData{}
	usedAnalyzer := akari.AnalyzerConfig{}
	for _, analyzer := range config.Load().Analyzers {
		if logType == analyzer.Name {
			usedAnalyzer = analyzer

			result, err := akari.Analyze(akari.AnalyzeOptions{
				Config:  analyzer,
				Source:  logFile,
				HasPrev: hasPrev,
				Prev:    prevLogFile,
				Logger:  slog.Default(),
				Seed:    serverData.HashSeed,
			})
			if err != nil {
				http.Error(w, "Failed to analyze log", http.StatusInternalServerError)
				return
			}

			tableData = result.Html(akari.HtmlOptions{
				ShowRank:    analyzer.ShowRank,
				DiffHeaders: analyzer.Diffs,
			})
			break
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err = serverData.TemplateFiles.ExecuteTemplate(w, "view.html", map[string]any{
		"Title":     filePath,
		"PrevPath":  prevFilePath,
		"LogType":   logType,
		"Config":    usedAnalyzer,
		"TableData": tableData,
		"toStyle": func(style map[string]string) string {
			result := []string{}
			for key, value := range style {
				result = append(result, fmt.Sprintf("%v:%v", key, value))
			}

			return strings.Join(result, ";")
		},
		"toAttrs": func(attrs map[string]string) template.HTMLAttr {
			result := []string{}
			for key, value := range attrs {
				result = append(result, fmt.Sprintf(`%v="%v"`, key, value))
			}

			return template.HTMLAttr(strings.Join(result, " "))
		},
	}); err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		log.Println("Template execution error:", err)
		return
	}
}

func filterViewHandler(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, "Failed to open file", http.StatusInternalServerError)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key not specified", http.StatusBadRequest)
		return
	}

	serverData := UseServerData(r)

	columns := akari.LogRecordColumns{}
	filtered := akari.LogRecordRows{}
	usedAnalyzer := akari.AnalyzerConfig{}
	for _, analyzer := range config.Load().Analyzers {
		if logType == analyzer.Name {
			usedAnalyzer = analyzer

			parseOptions, err := analyzer.ParseOptions(serverData.HashSeed)
			if err != nil {
				http.Error(w, "Failed to get parse options", http.StatusInternalServerError)
			}

			parsed, err := akari.Parse(parseOptions, logFile, slog.Default())
			if err != nil {
				http.Error(w, "Failed to analyze log", http.StatusInternalServerError)
				return
			}

			filtered = parsed.Records[key]
			columns = parsed.Columns
			break
		}
	}

	_ = usedAnalyzer

	groupByTimestamp := map[string]akari.LogRecordRows{}
	for _, record := range filtered {
		timestamp := record[columns.GetIndex("Timestamp")]
		if timestamp == nil {
			continue
		}

		timestampStr := fmt.Sprintf("%v", timestamp)
		groupByTimestamp[timestampStr] = append(groupByTimestamp[timestampStr], record)
	}

	maxCount := 0
	entries := []struct {
		Timestamp string
		Records   akari.LogRecordRows
	}{}
	for timestamp, records := range groupByTimestamp {
		entries = append(entries, struct {
			Timestamp string
			Records   akari.LogRecordRows
		}{
			Timestamp: timestamp,
			Records:   records,
		})

		if len(records) > maxCount {
			maxCount = len(records)
		}
	}

	slices.SortStableFunc(entries, func(a, b struct {
		Timestamp string
		Records   akari.LogRecordRows
	}) int {
		return strings.Compare(a.Timestamp, b.Timestamp)
	})

	tableHeaders := []akari.HtmlTableHeader{}
	tableHeaders = append(tableHeaders, akari.HtmlTableHeader{
		Text: "Count",
		Attributes: map[string]string{
			"data-colorize": fmt.Sprintf("%v", maxCount),
		},
	})
	for _, column := range columns {
		tableHeaders = append(tableHeaders, akari.HtmlTableHeader{
			Text: column.Name,
		})
	}

	tableRows := []akari.HtmlTableRow{}
	for _, entry := range entries {
		cells := []akari.HtmlTableCell{}

		row := entry.Records[0]
		cells = append(cells, akari.HtmlTableCell{
			Text: template.HTML(fmt.Sprintf("%v", len(entry.Records))),
			Attributes: map[string]string{
				"data-value": fmt.Sprintf("%v", len(entry.Records)),
			},
		})
		for _, column := range columns {
			cells = append(cells, akari.HtmlTableCell{
				Text: template.HTML(fmt.Sprintf("%v", row[columns.GetIndex(column.Name)])),
			})
		}

		tableRows = append(tableRows, akari.HtmlTableRow{
			Cells: cells,
		})
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err = serverData.TemplateFiles.ExecuteTemplate(w, "filter.html", map[string]any{
		"Title":   filePath,
		"LogType": logType,
		"Config":  usedAnalyzer,
		"TableData": akari.HtmlTableData{
			Headers: tableHeaders,
			Rows:    tableRows,
		},
		"toStyle": func(style map[string]string) string {
			result := []string{}
			for key, value := range style {
				result = append(result, fmt.Sprintf("%v:%v", key, value))
			}

			return strings.Join(result, ";")
		},
		"toAttrs": func(attrs map[string]string) template.HTMLAttr {
			result := []string{}
			for key, value := range attrs {
				result = append(result, fmt.Sprintf(`%v="%v"`, key, value))
			}

			return template.HTMLAttr(strings.Join(result, " "))
		},
	}); err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		log.Println("Template execution error:", err)
		return
	}
}

type ContextKey string

const contextKey ContextKey = "serverData"

type ServerData struct {
	TemplateFiles *template.Template
	HashSeed      uint64
}

func withServerData(next http.Handler, data ServerData) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), contextKey, data)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func UseServerData(r *http.Request) ServerData {
	return r.Context().Value(contextKey).(ServerData)
}

type ServeOptions struct {
	ConfigFile    string
	LogDir        string
	TemplateFiles *template.Template
	PublicFS      fs.FS
	HashSeed      uint64
	Port          int
	Hostname      string
}

func Serve(options ServeOptions) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}
	defer watcher.Close()

	go func() {
		for event := range watcher.Events {
			if event.Name == options.ConfigFile {
				var c akari.AkariConfig
				if _, err := toml.DecodeFile(options.ConfigFile, &c); err != nil {
					slog.Error("Failed to load config", "error", err)
					continue
				}

				slog.Info("Config reloaded")

				config.Store(c)
			}
		}
	}()

	var c akari.AkariConfig
	if _, err := toml.DecodeFile(options.ConfigFile, &c); err != nil {
		slog.Error("Failed to load config", "error", err)
	}

	config.Store(c)

	if err := watcher.Add(options.ConfigFile); err != nil {
		slog.Error("Failed to watch config file", "error", err)
	}

	slog.Debug("Loaded config", "path", options.ConfigFile, "config", config)

	mux := http.NewServeMux()

	mux.Handle("/public/", http.FileServer(http.FS(options.PublicFS)))
	mux.HandleFunc("/", logGroupHandler)
	mux.HandleFunc("/raw", rawFileHandler)
	mux.HandleFunc("/view", viewFileHandler)
	mux.HandleFunc("/filter", filterViewHandler)

	slog.Info("Starting server", "url", fmt.Sprintf("http://%v:%v", options.Hostname, options.Port))

	if err := http.ListenAndServe(
		fmt.Sprintf("%v:%v", options.Hostname, options.Port),
		withServerData(mux, ServerData{
			TemplateFiles: options.TemplateFiles,
			HashSeed:      options.HashSeed,
		}),
	); err != nil {
		slog.Error("Failed to start server", "error", err)
	}

	return nil
}
