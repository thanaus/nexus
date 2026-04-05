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

// Execute is the entry point called by main. It creates a signal-aware context
// cancelled on SIGINT, passes it to the root command via ExecuteContext, and
// ensures the signal handler is released via defer — regardless of whether the
// command succeeds, fails, or is interrupted. Cobra propagates the context to
// every subcommand automatically (cmd.ctx == nil → cmd.ctx = parent.ctx).
func Execute() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return NewRootCmd().ExecuteContext(ctx)
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
//
// Note: Cobra does not chain PersistentPreRunE across parent/child by default
// (EnableTraverseRunHooks is false). Sub-commands that need their own
// PersistentPreRunE must call initConfig() explicitly, or EnableTraverseRunHooks
// must be set to true before NewRootCmd() is called.
func setupRootCommand(root *cobra.Command) {
	root.AddGroup(
		&cobra.Group{ID: groupCore, Title: "Core Commands:"},
		&cobra.Group{ID: groupOther, Title: "Other Commands:"},
	)
	root.SetHelpCommand(newHelpCmd(root))
	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		return initConfig()
	}
}

// newHelpCmd returns the custom help command. It receives root as a parameter
// so it can delegate help rendering without relying on a package-level variable.
func newHelpCmd(root *cobra.Command) *cobra.Command {
	return &cobra.Command{
		Use:     "help [command]",
		Short:   "Show help for a command",
		GroupID: groupOther,
		RunE: func(c *cobra.Command, args []string) error {
			root.SetOut(c.OutOrStdout())
			root.SetErr(c.ErrOrStderr())
			if len(args) == 0 {
				return root.Help()
			}
			child, _, err := root.Find(args)
			if err != nil {
				return err
			}
			child.SetOut(c.OutOrStdout())
			child.SetErr(c.ErrOrStderr())
			return child.Help()
		},
	}
}

// initConfig is called before every command via PersistentPreRunE on root.
// Place cross-cutting initialisation here: logging setup, config file parsing, telemetry, etc.
func initConfig() error {
	// Reserved for future use.
	return nil
}
