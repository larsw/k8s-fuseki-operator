package main

import (
	"fmt"
	"os"

	"github.com/larsw/k8s-fuseki-operator/pkg/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
