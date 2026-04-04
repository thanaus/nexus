package cmd

import (
	"nexus/internal/app"

	"github.com/spf13/cobra"
)

// RootCmd is the root Cobra command for the CLI. Subcommands register via init()
// in their own files (ls.go, sync.go, …).
var RootCmd = &cobra.Command{
	Use:   app.Name,
	Short: app.DisplayName + " — Your data, perfectly in sync, everywhere.",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	// No Run: root is not Runnable, so usage omits "nexus [flags]" and plain `nexus` still shows help
	// (cobra returns flag.ErrHelp when the matched command has no Run).
}

// Execute runs the root command (entry point for main).
func Execute() error {
	return RootCmd.Execute()
}
