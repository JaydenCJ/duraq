// Command duraq is a durable message queue served over plain HTTP.
package main

import (
	"os"

	"github.com/JaydenCJ/duraq/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
