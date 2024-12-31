package cmd

import (
	"fmt"
	"io"
	"io/fs"
	"os"
)

type InitOptions struct {
	AkariInitFS fs.FS
}

func Init(options InitOptions) error {
	initFile, err := options.AkariInitFS.Open("akari.init.toml")
	if err != nil {
		return fmt.Errorf("failed to open init file: %w", err)
	}

	file, err := os.Create("akari.toml")
	if err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}

	if _, err := io.Copy(file, initFile); err != nil {
		return fmt.Errorf("failed to copy init file: %w", err)
	}

	return nil
}
