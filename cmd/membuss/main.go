// Command membuss is the Membuss daemon entry point.
//
// Phase 0: it only loads configuration and prints a startup banner.
// The real subsystem wiring (host, DHT, PEX, Memex, store, gateway)
// is added in later phases.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/nnlgsakib/membuss/config"
)

func main() {
	cfgPath := flag.String("config", "membuss.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("membuss: %v", err)
	}

	fmt.Fprintf(os.Stdout,
		"membuss daemon starting\n"+
			"  config:           %s\n"+
			"  data_dir:         %s\n"+
			"  listen_addrs:     %v\n"+
			"  bootstrap_peers:  %d\n"+
			"  gateway_addr:     %s\n"+
			"  api_addr:         %s\n"+
			"  grpc_addr:        %s\n"+
			"  anchor_mode:      %t\n"+
			"  reprovide:        %s\n",
		*cfgPath, cfg.DataDir, cfg.ListenAddrs, len(cfg.BootstrapPeers),
		cfg.GatewayAddr, cfg.APIAddr, cfg.GRPCAddr, cfg.AnchorMode,
		cfg.ReprovideInterval,
	)
}
