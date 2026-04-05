package cmd

import (
	"context"
	"fmt"
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
// command succeeds, fails, or is interrupted. Cobra propagates the context to
// every subcommand automatically (cmd.ctx == nil → cmd.ctx = parent.ctx).
//
// SilenceErrors and SilenceUsage are set on root so that:
//   - runtime errors (e.g. NATS connection failure) do not trigger a spurious
//     usage dump — usage is only relevant for mis-typed commands, not runtime failures;
//   - the application, not Cobra, controls when and how the error is displayed.
//
// The error is printed here via root.ErrOrStderr() to stay consistent with the
// injectable writer pattern used across all commands.
func Execute() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	root := NewRootCmd()
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(root.ErrOrStderr(), err)
		return err
	}
	return nil
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
// and cross-cutting initialisation hooks. Mirrors the pattern used by rclone.
//
// initConfig is wired via PersistentPreRunE rather than cobra.OnInitialize:
// OnInitialize appends to a package-level global (cobra.initializers) that is
// never reset, so repeated calls to NewRootCmd() — e.g. in parallel tests —
// would accumulate duplicate initialisers. PersistentPreRunE is stored on the
// Command instance itself and is therefore fully isolated per call.
func setupRootCommand(root *cobra.Command) {
	root.SilenceErrors = true
	root.SilenceUsage = true
	root.AddGroup(
		&cobra.Group{ID: groupCore, Title: "Core Commands:"},
		&cobra.Group{ID: groupOther, Title: "Other Commands:"},
	)
	// SetHelpCommandGroupID places the auto-generated help command in the
	// "Other Commands:" section. No custom help command is needed: Cobra's
	// default already handles sub-command lookup and writer propagation via
	// the parent chain (OutOrStdout walks up to root automatically).
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
