// Command infersched is the InferSched runtime coordinator entrypoint.
//
// Phase 1 scope: this binary validates the build wiring and the workload-trace
// tooling. The HTTP API, admission controller, resource manager and simulator
// backend are wired in through the coordinator in later phases.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BibekP/infersched/internal/workload"
)

const version = "0.1.0-phase1"

func main() {
	var (
		showVersion = flag.Bool("version", false, "print version and exit")
		tracePath   = flag.String("validate-trace", "", "validate a workload JSONL trace and print a summary")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("infersched %s\n", version)
		return
	}

	if *tracePath != "" {
		if err := validateTrace(*tracePath); err != nil {
			fmt.Fprintf(os.Stderr, "infersched: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("infersched %s\n", version)
	fmt.Println("Phase 1 build: scheduling core, lifecycle and trace tooling are available.")
	fmt.Println("Use -validate-trace <file> to check a workload trace.")
}

func validateTrace(path string) error {
	tr, err := workload.Load(path)
	if err != nil {
		return err
	}
	h := tr.Header
	fmt.Printf("trace %s: valid %s\n", path, h.FormatVersion)
	fmt.Printf("  profile        : %s\n", h.Profile)
	fmt.Printf("  seed           : %d\n", h.Seed)
	fmt.Printf("  requests       : %d\n", len(tr.Records))
	var withDeadline, totalPrompt, totalOutput int
	for _, r := range tr.Records {
		if r.Deadline != 0 {
			withDeadline++
		}
		totalPrompt += r.PromptTokens
		totalOutput += r.ExpectedOutput
	}
	fmt.Printf("  with deadline  : %d\n", withDeadline)
	fmt.Printf("  total prompt tk: %d\n", totalPrompt)
	fmt.Printf("  total output tk: %d\n", totalOutput)
	return nil
}
