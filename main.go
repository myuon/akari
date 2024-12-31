package main

import (
	"embed"
	"fmt"
	"html/template"
	"log"
	"log/slog"
	"os"

	"github.com/akamensky/argparse"
	"github.com/myuon/akari/akari"
	"github.com/myuon/akari/cmd"
)

//go:embed templates/*
var templateFS embed.FS

//go:embed akari.init.toml
var akariInitFS embed.FS

//go:embed public/*
var publicFS embed.FS

var (
	templateFiles     = template.Must(template.ParseFS(templateFS, "templates/*.html"))
	globalSeed        = uint64(0xdeadbeef)
	defaultConfigPath = "akari.toml"
)

func main() {
	parser := argparse.NewParser("akari", "Log analyzer")
	verbose := parser.Flag("v", "verbose", &argparse.Options{Help: "Verbose mode"})

	initCommand := parser.NewCommand("init", "Generates a new akari configuration file")

	runCommand := parser.NewCommand("run", "Run the log analyzer")
	runConfigFile := runCommand.String("c", "akari.toml", &argparse.Options{Help: "Configuration file path"})
	runLogFile := runCommand.StringPositional(nil)

	serveCommand := parser.NewCommand("serve", "Starts a web server to serve the log analyzer")
	serverConfigFile := serveCommand.String("c", "akari.toml", &argparse.Options{Help: "Configuration file path"})
	logDir := serveCommand.StringPositional(nil)

	if err := parser.Parse(os.Args); err != nil {
		fmt.Print(parser.Usage(err))
	}

	if *verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	if initCommand.Happened() {
		if err := cmd.Init(cmd.InitOptions{
			AkariInitFS: akariInitFS,
		}); err != nil {
			log.Fatal(err)
		}
	} else if runCommand.Happened() {
		if err := cmd.Run(cmd.RunOptions{
			ConfigFile: akari.StringOr(*runConfigFile, defaultConfigPath),
			LogFile:    *runLogFile,
			GlobalSeed: globalSeed,
		}); err != nil {
			log.Fatal(err)
		}
	} else if serveCommand.Happened() {
		if err := cmd.Serve(cmd.ServeOptions{
			ConfigFile:    akari.StringOr(*serverConfigFile, defaultConfigPath),
			LogDir:        *logDir,
			TemplateFiles: templateFiles,
			PublicFS:      publicFS,
			HashSeed:      globalSeed,
		}); err != nil {
			log.Fatal(err)
		}
	}
}
