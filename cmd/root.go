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

func init() {
	// EnableTraverseRunHooks makes Cobra chain PersistentPreRunE across
	// parent/child commands. Without it, a sub-command that defines its own
	// PersistentPreRunE would silently skip the root hook (initConfig).
	// Set here via init() so it applies unconditionally at package import —
	// in production via Execute() and in tests via NewRootCmd() — without
	// depending on call order inside setupRootCommand.
	cobra.EnableTraverseRunHooks = true
}

// Execute is the entry point called by main. It creates a signal-aware context
// cancelled on SIGINT, passes it to the root command via ExecuteContext, and
// ensures the signal handler is released via defer — regardless of whether the
// command succeeds, fails, or is interrupted.
//
// Error printing strategy — Cobra owns all error output:
//   - Arg/flag validation errors: Cobra prints "Error: ..." then the usage block.
//   - Runtime errors (from RunE): each RunE sets cmd.SilenceUsage = true so
//     Cobra prints "Error: ..." without the usage block.
//
// SilenceErrors is not set (defaults to false) so Cobra always prints the error.
// We do not reprint it here to avoid double output.
func Execute() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	root := NewRootCmd()
	return root.ExecuteContext(ctx)
}

// NewRootCmd builds and returns a fresh root command with all subcommands
// attached. Returning a new instance on each call avoids shared global state,
// making it safe to call from parallel tests.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   app.Name,
		Short: app.DisplayName + " — Your data, perfectly in sync, everywhere.",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
	setupRootCommand(root)
	root.AddCommand(newLsCmd(), newSyncCmd())
	return root
}

// setupRootCommand centralises all Cobra configuration: groups, help command,
// and cross-cutting initialisation hooks.
//
// SilenceErrors is not set: Cobra prints "Error: ..." for every failure.
// SilenceUsage  is intentionally NOT set here: each RunE sets it to true at
// its very first line so that:
//   - arg/flag validation errors (Args runs before RunE) still print usage;
//   - runtime errors inside RunE (e.g. NATS failure) do not.
func setupRootCommand(root *cobra.Command) {
	root.AddGroup(
		&cobra.Group{ID: groupCore, Title: "Core Commands:"},
		&cobra.Group{ID: groupOther, Title: "Other Commands:"},
	)
	root.SetHelpCommandGroupID(groupOther)
	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		return initConfig()
	}
}

// initConfig is called before every command via PersistentPreRunE on root.
// Place cross-cutting initialisation here: logging setup, config file parsing, telemetry, etc.
func initConfig() error {
	// Reserved for future use.
	return nil
}
