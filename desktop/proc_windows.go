//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

// hideConsoleWindow sets creation flags on Windows to prevent
// a console window from appearing when spawning a child process.
func hideConsoleWindow(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: 0x08000000, // CREATE_NO_WINDOW
	}
}
