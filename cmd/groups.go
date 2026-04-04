package cmd

import (
	"github.com/spf13/cobra"
)

const (
	groupCore  = "core"
	groupOther = "other"
)

func init() {
	RootCmd.AddGroup(&cobra.Group{ID: groupCore, Title: "Core Commands:"})
	RootCmd.AddGroup(&cobra.Group{ID: groupOther, Title: "Other Commands:"})
	// Register as the single help command (do not AddCommand: InitDefaultHelpCmd would add a second "help").
	RootCmd.SetHelpCommand(newHelpCmd())
}

func newHelpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "help [command]",
		Short: "Show help for a command",
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
