package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

var (
	// Version is the semantic version of the application.
	// Can be overridden at build time via:
	// -ldflags "-X github.com/nnlgsakib/membuss/core/version.Version=1.0.0"
	Version = "0.4.1"

	// GitCommit is the git commit SHA.
	// Can be overridden at build time via:
	// -ldflags "-X github.com/nnlgsakib/membuss/core/version.GitCommit=$(git rev-parse HEAD)"
	GitCommit = ""

	// BuildTime is the timestamp of when the build occurred.
	// Can be overridden at build time via:
	// -ldflags "-X github.com/nnlgsakib/membuss/core/version.BuildTime=$(date)"
	BuildTime = ""
)

func init() {
	// Try to populate GitCommit and BuildTime from Go build info if they are empty
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if GitCommit == "" {
					GitCommit = setting.Value
				}
			case "vcs.time":
				if BuildTime == "" {
					BuildTime = setting.Value
				}
			}
		}
	}
}

// String returns a formatted version details string.
func String() string {
	commit := GitCommit
	if len(commit) > 7 {
		commit = commit[:7]
	}
	if commit == "" {
		commit = "unknown"
	}
	buildTime := BuildTime
	if buildTime == "" {
		buildTime = "unknown"
	}
	return fmt.Sprintf("membuss version %s (commit %s, built %s, %s/%s)", Version, commit, buildTime, runtime.GOOS, runtime.GOARCH)
}
