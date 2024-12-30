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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/akamensky/argparse"
	"github.com/fsnotify/fsnotify"
	"github.com/myuon/akari/akari"
)

var (
	templateFiles  = template.Must(template.ParseGlob("templates/*.html"))
	rootDir        = "."
	configFilePath = "akari.toml"
	config         = akari.NewGlobalVar(akari.AkariConfig{})
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
	LogType string
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

	entries := []PageDataFile{}
	for logType, files := range filesByType {
		entries = append(entries, PageDataFile{
			LogType: logType,
			Content: files,
		})
	}

	logTypes := config.Load().GetLogTypes()

	slices.SortStableFunc(entries, func(a, b PageDataFile) int {
		aIndex := slices.Index(logTypes, a.LogType)
		bIndex := slices.Index(logTypes, b.LogType)

		if aIndex == -1 {
			return 1
		} else if bIndex == -1 {
			return -1
		} else {
			return aIndex - bIndex
		}
	})

	pageData := PageData{
		Title: "Akari",
		Files: entries,
	}
	if err = templateFiles.ExecuteTemplate(w, "files.html", pageData); err != nil {
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

	tableData := akari.TableData{}
	usedAnalyzer := akari.AnalyzerConfig{}
	for _, analyzer := range config.Load().Analyzers {
		if logType == analyzer.Name {
			usedAnalyzer = analyzer
			tableData = akari.Analyze(analyzer, logFile, prevLogFile, slog.Default())
			break
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err = templateFiles.ExecuteTemplate(w, "view.html", map[string]any{
		"Title":     filePath,
		"LogType":   logType,
		"Config":    usedAnalyzer,
		"TableData": tableData,
	}); err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		log.Println("Template execution error:", err)
		return
	}
}

func main() {
	parser := argparse.NewParser("akari", "Log analyzer")
	verbose := parser.Flag("v", "verbose", &argparse.Options{Help: "Verbose mode"})

	initCommand := parser.NewCommand("init", "Generates a new akari configuration file")

	runCommand := parser.NewCommand("run", "Run the log analyzer")
	runConfigFile := runCommand.String("c", "akari.toml", &argparse.Options{Help: "Configuration file path"})
	runLogFile := runCommand.StringPositional(nil)

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
		initFile, err := os.Open("akari.init.toml")
		if err != nil {
			log.Fatal(err)
		}

		file, err := os.Create("akari.toml")
		if err != nil {
			log.Fatal(err)
		}

		if _, err := io.Copy(file, initFile); err != nil {
			log.Fatal(err)
		}
	} else if runCommand.Happened() {
		configFilePath = *runConfigFile
		logFilePath := *runLogFile

		logger := akari.NewDurationLogger(slog.Default())

		logger.Debug("Loading config", "path", configFilePath)

		var c akari.AkariConfig
		if _, err := toml.DecodeFile(configFilePath, &c); err != nil {
			log.Fatal(err)
		}

		config.Store(c)

		logFile, err := os.Open(logFilePath)
		if err != nil {
			log.Fatal(err)
		}

		logger.Debug("Loaded config", "config", config)

		line := make([]byte, 512)
		n, err := logFile.Read(line)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		line = line[:n]

		if strings.Contains(string(line), "\n") {
			line = []byte(strings.SplitN(string(line), "\n", 2)[0])
		}

		logFile.Seek(0, 0)

		logger.Debug("Read first line", "line", string(line))

		tableData := akari.TableData{}
		for _, analyzer := range config.Load().Analyzers {
			if analyzer.Parser.RegExp.Match(line) {
				logger.Debug("Matched analyzer", "analyzer", analyzer.Name)

				tableData = akari.Analyze(analyzer, logFile, nil, logger)
				break
			}

			logger.Debug("Skipped analyzer", "analyzer", analyzer.Name)
		}

		logger.Debug("Analyzed log")

		tableData.Write(os.Stdout)

		logger.Debug("Printed table")
	} else if serveCommand.Happened() {
		rootDir = *logDir
		configFilePath = *configFile

		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Fatal(err)
		}
		defer watcher.Close()

		go func() {
			for {
				select {
				case event, ok := <-watcher.Events:
					if !ok {
						return
					}

					if event.Name == configFilePath {
						var c akari.AkariConfig
						if _, err := toml.DecodeFile(configFilePath, &c); err != nil {
							log.Fatal(err)
						}

						slog.Info("Config reloaded")

						config.Store(c)
					}
				}
			}
		}()

		var c akari.AkariConfig
		if _, err := toml.DecodeFile(configFilePath, &c); err != nil {
			log.Fatal(err)
		}

		config.Store(c)

		if err := watcher.Add(configFilePath); err != nil {
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
