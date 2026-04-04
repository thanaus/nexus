package main

import (
	"errors"
	"fmt"
	"os"

	"nexus/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		var already *cmd.UsageAlreadyReported
		if !errors.As(err, &already) {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		os.Exit(1)
	}
}
