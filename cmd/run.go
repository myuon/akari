package cmd

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/myuon/akari/akari"
)

type RunOptions struct {
	ConfigFile string
	LogFile    string
	GlobalSeed uint64
	Writer     io.Writer
}

func Run(options RunOptions) error {
	configFilePath := options.ConfigFile
	logFilePath := options.LogFile

	logger := akari.NewDurationLogger(slog.Default())

	logger.Debug("Loading config", "path", configFilePath)

	var config akari.AkariConfig
	if _, err := toml.DecodeFile(configFilePath, &config); err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	logFile, err := os.Open(logFilePath)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	logger.Debug("Loaded config", "config", config)

	line := make([]byte, 512)
	n, err := logFile.Read(line)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read log file: %w", err)
	}
	line = line[:n]

	if strings.Contains(string(line), "\n") {
		line = []byte(strings.SplitN(string(line), "\n", 2)[0])
	}

	if _, err := logFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to rewind log file: %w", err)
	}

	logger.Debug("Read first line", "line", string(line))

	tableData := akari.TableData{}
	for _, analyzer := range config.Analyzers {
		if analyzer.Parser.RegExp.Match(line) {
			logger.Debug("Matched analyzer", "analyzer", analyzer.Name)

			result, err := akari.Analyze(akari.AnalyzeOptions{
				Config:  analyzer,
				Source:  logFile,
				HasPrev: false,
				Prev:    nil,
				Logger:  logger,
				Seed:    options.GlobalSeed,
			})
			if err != nil {
				return fmt.Errorf("failed to analyze: %w", err)
			}

			tableData = result
			break
		}

		logger.Debug("Skipped analyzer", "analyzer", analyzer.Name)
	}

	logger.Debug("Analyzed log")

	tableData.Write(options.Writer)

	logger.Debug("Printed table")

	return nil
}
