// Command isolation is Supaflow's out-of-process user-code runner.
package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"github.com/pazhou/supaflow/internal/isolation"
)

func main() {
	if isolation.IsResourceLimitLauncher(os.Args[1:]) {
		os.Exit(isolation.RunResourceLimitLauncher(os.Args[1:]))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if isolation.IsCapabilityCheck(os.Args[1:]) {
		if err := isolation.CheckCapabilities(ctx); err != nil {
			_, _ = os.Stderr.WriteString("isolation capability check failed\n")
			os.Exit(125)
		}
		return
	}
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	var request isolation.Request
	if err := decoder.Decode(&request); err != nil {
		_ = json.NewEncoder(os.Stdout).Encode(isolation.Response{Code: "invalid_protocol"})
		return
	}
	_ = json.NewEncoder(os.Stdout).Encode(isolation.Run(ctx, request))
}
