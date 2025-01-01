package main

import (
	"embed"
	"fmt"
	"html/template"
	"log"
	"log/slog"
	"os"
	"strconv"

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

type RunCommand struct {
	Command    *argparse.Command
	ConfigFile *string
	LogFile    *string
}

func NewRunCommand(parser *argparse.Parser) *RunCommand {
	command := parser.NewCommand("run", "Run the log analyzer")
	condfig := command.String("c", "akari.toml", &argparse.Options{Help: "Configuration file path"})
	file := command.StringPositional(nil)

	return &RunCommand{
		Command:    command,
		ConfigFile: condfig,
		LogFile:    file,
	}
}

type ServeCommand struct {
	Command    *argparse.Command
	ConfigFile *string
	LogDir     *string
}

func NewServeCommand(parser *argparse.Parser) *ServeCommand {
	command := parser.NewCommand("serve", "Starts a web server to serve the log analyzer")
	config := command.String("c", "akari.toml", &argparse.Options{Help: "Configuration file path"})
	logDir := command.StringPositional(nil)

	return &ServeCommand{
		Command:    command,
		ConfigFile: config,
		LogDir:     logDir,
	}
}

func main() {
	parser := argparse.NewParser("akari", "Log analyzer")
	verbose := parser.Flag("v", "verbose", &argparse.Options{Help: "Verbose mode"})

	initCommand := parser.NewCommand("init", "Generates a new akari configuration file")
	runCommand := NewRunCommand(parser)
	serveCommand := NewServeCommand(parser)

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
	} else if runCommand.Command.Happened() {
		if err := cmd.Run(cmd.RunOptions{
			ConfigFile: akari.StringOr(*runCommand.ConfigFile, defaultConfigPath),
			LogFile:    *runCommand.LogFile,
			GlobalSeed: globalSeed,
			Writer:     os.Stdout,
		}); err != nil {
			log.Fatal(err)
		}
	} else if serveCommand.Command.Happened() {
		hostName := "localhost"
		if val, ok := os.LookupEnv("HOSTNAME"); ok {
			hostName = val
		}

		port := 8089
		if val, ok := os.LookupEnv("PORT"); ok {
			port, _ = strconv.Atoi(val)
		}

		if err := cmd.Serve(cmd.ServeOptions{
			ConfigFile:    akari.StringOr(*serveCommand.ConfigFile, defaultConfigPath),
			LogDir:        *serveCommand.LogDir,
			TemplateFiles: templateFiles,
			PublicFS:      publicFS,
			HashSeed:      globalSeed,
			Port:          port,
			Hostname:      hostName,
		}); err != nil {
			log.Fatal(err)
		}
	}
}
