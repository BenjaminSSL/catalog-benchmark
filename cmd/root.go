package cmd

import (
	"flag"
	"github.com/joho/godotenv"
	"log"
	"os"
)

type Command struct {
	Name        string
	Description string
	Flags       *flag.FlagSet // Set of flags for the given command
	Handler     func() error
}

var commands = make(map[string]*Command)

func RegisterCommand(command *Command) {
	commands[command.Name] = command
}

func Run() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading the enviroment variabels files .env")
	}

	args := os.Args[1:]
	// TODO: replace with proper help default command
	if len(args) == 0 {
		log.Println("No command to execute")
		os.Exit(1)
	}

	cmdName := args[0]
	command, exists := commands[cmdName]
	if !exists {
		log.Println("No such command: ", cmdName)
		os.Exit(1)
	}

	if err := command.Flags.Parse(args[1:]); err != nil {
		os.Exit(1)
	}

	if err := command.Handler(); err != nil {
		log.Fatalf("Error executing command: %v", err)
	}

}
