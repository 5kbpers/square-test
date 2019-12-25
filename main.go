package main

import (
	"os"

	"github.com/5kbpers/test1/cmd"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

func main() {
	command := &cobra.Command{
		Use:              "test",
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	command.AddCommand(cmd.NewRunCommand(), cmd.NewLoadCommand())
	command.SetArgs(os.Args[1:])
	if err := command.Execute(); err != nil {
		command.Println(errors.ErrorStack(err))
		os.Exit(1)
	}
}
