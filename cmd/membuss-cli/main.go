// Command membuss-cli is the user-facing CLI for the Membuss daemon.
//
// Phase 0: it only resolves a config path and prints it. Command
// implementations are added in later phases.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	cfgPath := flag.String("config", "membuss.yaml", "path to YAML config file (used to locate the daemon gRPC endpoint)")
	flag.Parse()

	fmt.Fprintf(os.Stdout, "membuss-cli: would connect to daemon via config %q\n", *cfgPath)
}
