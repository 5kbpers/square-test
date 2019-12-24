package main

import (
	"os"

	"github.com/5kbpers/test1/cmd"
	"github.com/pingcap/errors"
)

func main() {
	command := cmd.NewTestCommand()
	command.SetArgs(os.Args[1:])
	if err := command.Execute(); err != nil {
		command.Println(errors.ErrorStack(err))
		os.Exit(1)
	}
}
