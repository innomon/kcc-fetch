package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
)

// Command defines the interface for a runnable command.
type Command interface {
	Execute(context.Context) error
	Help() string
}

// commands holds the registered commands.
var commands = make(map[string]Command)

// RegisterCommand adds a command to the registry.
func RegisterCommand(name string, cmd Command) {
	commands[name] = cmd
}

// helpCmd implements the Command interface for the 'help' command.
type helpCmd struct{}

// Execute prints usage for all commands, or for a specific command if provided.
func (c *helpCmd) Execute(ctx context.Context) error {
	if len(os.Args) > 2 {
		subCmdName := os.Args[2]
		cmd, ok := commands[subCmdName]
		if !ok {
			fmt.Printf("Error: Unknown command '%s'\n\n", subCmdName)
			printUsage()
			os.Exit(1)
		}
		fmt.Printf("Usage: kcc %s [arguments]\n\n", subCmdName)
		fmt.Printf("  %s\n", cmd.Help())
	} else {
		printUsage()
	}
	return nil
}

// Help returns the help string for the help command.
func (c *helpCmd) Help() string {
	return "Shows a list of commands or help for a specific command."
}

func init() {
	// Register all available commands.
	RegisterCommand("help", &helpCmd{})
	RegisterCommand("fetch", &fetchCmd{})
	RegisterCommand("export", &exportCmd{})
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if len(os.Args) < 2 {
		printUsage()
		return
	}

	cmdName := os.Args[1]
	cmd, ok := commands[cmdName]
	if !ok {
		fmt.Printf("Error: Unknown command '%s'\n\n", cmdName)
		printUsage()
		os.Exit(1)
	}

	// Pass context to Execute and handle error
	if err := cmd.Execute(ctx); err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			fmt.Println("Operation cancelled.")
			os.Exit(1)
		}
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: kcc <command> [arguments]")
	fmt.Println("\nAvailable commands:")

	var cmdNames []string
	for name := range commands {
		cmdNames = append(cmdNames, name)
	}
	sort.Strings(cmdNames)

	for _, name := range cmdNames {
		cmd := commands[name]
		fmt.Printf("  %-15s %s\n", name, cmd.Help())
	}
}
