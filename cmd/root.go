package cmd

import (
	"fmt"

	"nexus/internal/app"

	"github.com/spf13/cobra"
)

// UsageAlreadyReported is returned when usageError has already written
// "Error: …" and Usage to stderr; main must not print the error again.
type UsageAlreadyReported struct {
	Err error
}

func (e *UsageAlreadyReported) Error() string { return e.Err.Error() }
func (e *UsageAlreadyReported) Unwrap() error { return e.Err }

// usageError prints "Error:" first, then subcommand usage (natural CLI order).
// Subcommands should set SilenceErrors: true so Cobra does not append another Error line.
func usageError(cmd *cobra.Command, err error) error {
	fmt.Fprintf(cmd.OutOrStderr(), "Error: %v\n\n", err)
	_ = cmd.Usage()
	return &UsageAlreadyReported{Err: err}
}

// RootCmd is the root Cobra command for the CLI. Subcommands register via init()
// in their own files (ls.go, sync.go, …).
var RootCmd = &cobra.Command{
	Use:   app.Name,
	Short: app.DisplayName + " — Your data, perfectly in sync, everywhere.",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	// Let main print errors once (unknown subcommand, etc.); avoids duplicate with fmt in main.go.
	SilenceErrors: true,
	// No Run: root is not Runnable, so usage omits "nexus [flags]" and plain `nexus` still shows help
	// (cobra returns flag.ErrHelp when the matched command has no Run).
}

// Execute runs the root command (entry point for main).
func Execute() error {
	return RootCmd.Execute()
}
