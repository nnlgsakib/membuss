# Generate a starter membuss.yaml in the current directory.
$ErrorActionPreference = 'Stop'

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = (Resolve-Path (Join-Path $here '..')).Path

# Use `go run` so we do not need to build the daemon just to emit a config.
Push-Location $root
try {
    # We piggy-back on a tiny one-off: invoke `go run` with a file that
    # calls config.WriteDefault on the requested path.
    $tmp = New-TemporaryFile
    $program = @"
package main

import (
    "fmt"
    "os"

    "github.com/yourname/membuss/config"
)

func main() {
    if len(os.Args) < 2 {
        fmt.Fprintln(os.Stderr, "usage: init-config <path>")
        os.Exit(2)
    }
    if err := config.WriteDefault(os.Args[1]); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    fmt.Println("wrote", os.Args[1])
}
"@
    Set-Content -Path "$($tmp.FullName).go" -Value $program -Encoding utf8
    Move-Item -Path "$($tmp.FullName).go" -Destination (Join-Path $root 'scripts\init_config_main.go') -Force
    try {
        go run (Join-Path $root 'scripts\init_config_main.go') (Join-Path $root 'membuss.yaml')
    } finally {
        Remove-Item -Force -ErrorAction SilentlyContinue (Join-Path $root 'scripts\init_config_main.go')
    }
} finally {
    Pop-Location
}
