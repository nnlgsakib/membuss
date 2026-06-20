# Design: Release Workflow — Installers + Commit-based Release Notes

**Date:** 2026-06-20
**Status:** Approved
**Scope:** `.github/workflows/release.yml`

---

## Problem

The current release workflow only builds raw daemon/CLI binaries (tar.gz/zip). There are no desktop app installers, and release notes only include merge commits via `generate_release_notes: true`.

## Goals

1. Add Windows NSIS installer for the desktop app
2. Add Linux AppImage for the desktop app
3. Release body lists all commits between the previous tag and the current tag (not just merges)
4. Keep existing raw binary artifacts

---

## Release Notes Generation

Add a `generate-notes` job that runs before the build jobs:

- `git log --oneline ${PREV_TAG}..${TAG}` to get all commits since the last release
- Format as markdown with sections: `## What's Changed`, list of commits, `## New Contributors` if any
- Write to `release-notes.md`
- Pass to `softprops/action-gh-release` via `body_path` instead of `generate_release_notes: true`

## Desktop Windows Installer

- New job: `build-desktop-windows`
- Runs on `windows-latest`
- Installs: Go, Node.js, Wails CLI, NSIS
- Builds: `wails build --platform windows/amd64 --nsis`
- Output: `desktop/build/bin/Membuss-Setup-amd64.exe`
- Upload as artifact

## Desktop Linux AppImage

- New job: `build-desktop-linux`
- Runs on `ubuntu-latest`
- Installs: Go, Node.js, Wails CLI, appimagetool + FUSE deps
- Builds: `wails build --platform linux/amd64`
- Packages with appimagetool into `Membuss-amd64.AppImage`
- Upload as artifact

## Workflow Structure

```
generate-notes
build-binaries (linux-amd64, linux-arm64, windows-amd64)
build-desktop-windows
build-desktop-linux
    ↓ (all four above)
release (collects all artifacts + release notes)
```

## Files Changed

- `.github/workflows/release.yml` — rewrite with new jobs
