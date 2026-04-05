package cmd

import (
	"context"
	"os"
	"os/signal"

	"nexus/internal/app"

	"github.com/spf13/cobra"
)

const (
	groupCore  = "core"
	groupOther = "other"
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
	setupRootCommand(RootCmd)
	return RootCmd.Execute()
}

// setupRootCommand centralises all Cobra configuration: groups, help command,
// and cross-cutting initialisation hooks. Mirrors the pattern used by rclone.
func setupRootCommand(root *cobra.Command) {
	root.AddGroup(
		&cobra.Group{ID: groupCore, Title: "Core Commands:"},
		&cobra.Group{ID: groupOther, Title: "Other Commands:"},
	)
	root.SetHelpCommand(newHelpCmd())

	// PersistentPreRunE installs a signal-aware context on every command so
	// that RunE functions receive it via cmd.Context() instead of creating
	// their own. This is the idiomatic Cobra approach since v1.5.
	root.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		cmd.SetContext(ctx)
		// stop is called when the context is cancelled (signal received) or
		// when the command finishes — whichever comes first.
		go func() {
			<-ctx.Done()
			stop()
		}()
		return nil
	}

	cobra.OnInitialize(initConfig)
}

// newHelpCmd returns the custom help command registered via SetHelpCommand.
// It is defined here alongside setupRootCommand to keep all Cobra wiring in one place.
func newHelpCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "help [command]",
		Short:   "Show help for a command",
		GroupID: groupOther,
		RunE: func(c *cobra.Command, args []string) error {
			RootCmd.SetOut(c.OutOrStdout())
			RootCmd.SetErr(c.ErrOrStderr())
			if len(args) == 0 {
				return RootCmd.Help()
			}
			child, _, err := RootCmd.Find(args)
			if err != nil {
				return err
			}
			child.SetOut(c.OutOrStdout())
			child.SetErr(c.ErrOrStderr())
			return child.Help()
		},
	}
}

// initConfig is called by Cobra before any command runs (via OnInitialize).
// Place cross-cutting initialisation here: logging, config file loading, etc.
func initConfig() {
	// Reserved for future use: logging setup, config file parsing, telemetry…
}
