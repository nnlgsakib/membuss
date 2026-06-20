# Release Workflow — Installers + Commit-based Release Notes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Update the GitHub Actions release workflow to build Windows/Linux desktop installers, and generate release notes listing all commits between the previous and current tags.

**Architecture:** The existing `release.yml` is rewritten with four parallel build jobs (daemon binaries, desktop Windows, desktop Linux) plus a preceding notes-generation job and a final release job that collects all artifacts. Release notes are generated via `git log` between tags.

**Tech Stack:** GitHub Actions, Wails CLI, NSIS, appimagetool, Go, Node.js

---

## File Map

| File | Responsibility |
|------|---------------|
| `.github/workflows/release.yml` | Full release workflow — notes generation, binary builds, desktop builds, release creation |

---

### Task 1: Generate release notes from git log

**Files:**
- Modify: `.github/workflows/release.yml`

- [ ] **Step 1: Add the `generate-notes` job**

Add this job at the top of the workflow (before `build-binaries`). It determines the previous tag, runs `git log --oneline` between the two tags, and writes a markdown file:

```yaml
  generate-notes:
    name: Generate Release Notes
    runs-on: ubuntu-latest
    outputs:
      tag_name: ${{ steps.meta.outputs.tag_name }}
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Get tag info
        id: meta
        run: |
          TAG="${GITHUB_REF#refs/tags/}"
          echo "tag_name=$TAG" >> "$GITHUB_OUTPUT"

          # Find the previous tag
          PREV_TAG=$(git tag --sort=-version:refname | grep -A1 "^${TAG}$" | tail -1)
          if [ "$PREV_TAG" = "$TAG" ]; then
            PREV_TAG=$(git rev-list --max-parents=0 HEAD | head -1)
          fi
          echo "prev_tag=$PREV_TAG" >> "$GITHUB_OUTPUT"

      - name: Build release notes
        run: |
          TAG="${{ steps.meta.outputs.tag_name }}"
          PREV="${{ steps.meta.outputs.prev_tag }}"

          echo "# Membuss $TAG" > release-notes.md
          echo "" >> release-notes.md
          echo "## What's Changed" >> release-notes.md
          echo "" >> release-notes.md

          if [ "$PREV" != "$(git rev-list --max-parents=0 HEAD | head -1)" ]; then
            echo "Commits since $PREV:" >> release-notes.md
            echo "" >> release-notes.md
            git log --oneline --no-merges "$PREV..$TAG" >> release-notes.md
          else
            echo "Initial release." >> release-notes.md
            echo "" >> release-notes.md
            git log --oneline --no-merges >> release-notes.md
          fi

          echo "" >> release-notes.md
          echo "---" >> release-notes.md
          echo "**Full changelog**: https://github.com/${{ github.repository }}/compare/${PREV}...${TAG}" >> release-notes.md

      - name: Upload release notes
        uses: actions/upload-artifact@v4
        with:
          name: release-notes
          path: release-notes.md
          retention-days: 1
```

- [ ] **Step 2: Verify YAML syntax**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml'))"`
Expected: no output (valid YAML)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: add release notes generation from git log between tags"
```

---

### Task 2: Update build-binaries to depend on generate-notes

**Files:**
- Modify: `.github/workflows/release.yml` (the `build-binaries` job)

- [ ] **Step 1: Add `needs: generate-notes` to build-binaries**

The existing `build-binaries` job currently has `needs: build-frontend`. Change it to depend on both:

```yaml
  build-binaries:
    name: Build Go Binaries
    needs: [build-frontend, generate-notes]
    runs-on: ubuntu-latest
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: make build-binaries depend on generate-notes"
```

---

### Task 3: Add Windows desktop installer job

**Files:**
- Modify: `.github/workflows/release.yml`

- [ ] **Step 1: Add `build-desktop-windows` job**

Add this job after the existing `build-binaries` job:

```yaml
  build-desktop-windows:
    name: Build Desktop (Windows)
    needs: generate-notes
    runs-on: windows-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version-file: 'go.mod'
          cache: true

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: 'npm'
          cache-dependency-path: desktop/frontend/package-lock.json

      - name: Install Wails
        run: go install github.com/wailsapp/wails/v2/cmd/wails@latest

      - name: Install NSIS
        run: choco install nsis -y

      - name: Build desktop app (NSIS installer)
        working-directory: desktop
        env:
          CGO_ENABLED: 0
        run: |
          wails build --platform windows/amd64 --nsis

      - name: Rename installer
        shell: bash
        run: |
          TAG="${GITHUB_REF#refs/tags/}"
          cp desktop/build/bin/Membuss-Setup-amd64.exe \
             "Membuss-${TAG}-windows-amd64-installer.exe"

      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: desktop-windows-installer
          path: Membuss-*-windows-amd64-installer.exe
          retention-days: 1
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: add Windows desktop NSIS installer build job"
```

---

### Task 4: Add Linux desktop AppImage job

**Files:**
- Modify: `.github/workflows/release.yml`

- [ ] **Step 1: Add `build-desktop-linux` job**

Add this job after the `build-desktop-windows` job:

```yaml
  build-desktop-linux:
    name: Build Desktop (Linux)
    needs: generate-notes
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version-file: 'go.mod'
          cache: true

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: 'npm'
          cache-dependency-path: desktop/frontend/package-lock.json

      - name: Install Wails
        run: go install github.com/wailsapp/wails/v2/cmd/wails@latest

      - name: Install appimagetool dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y libgtk-3-0 libwebkit2gtk-4.0-37 \
            fuse libfuse2 wget file

      - name: Install appimagetool
        run: |
          wget -q https://github.com/AppImage/AppImageKit/releases/download/continuous/appimagetool-x86_64.AppImage -O /usr/local/bin/appimagetool
          chmod +x /usr/local/bin/appimagetool

      - name: Build desktop app
        working-directory: desktop
        env:
          CGO_ENABLED: 0
        run: |
          wails build --platform linux/amd64

      - name: Create AppDir structure
        run: |
          TAG="${GITHUB_REF#refs/tags/}"
          APPDIR="Membuss.AppDir"

          mkdir -p "$APPDIR/usr/bin"
          mkdir -p "$APPDIR/usr/share/applications"
          mkdir -p "$APPDIR/usr/share/icons/hicolor/256x256/apps"

          cp desktop/build/bin/desktop "$APPDIR/usr/bin/membuss"
          cp desktop/icon.png "$APPDIR/usr/share/icons/hicolor/256x256/apps/membuss.png"

          cat > "$APPDIR/membuss.desktop" << 'EOF'
          [Desktop Entry]
          Type=Application
          Name=Membuss
          Exec=membuss
          Icon=membuss
          Categories=Network;Utility;
          Terminal=false
          EOF

          cat > "$APPDIR/AppRun" << 'APPRUN'
          #!/bin/sh
          HERE="$(dirname "$(readlink -f "$0")")"
          exec "$HERE/usr/bin/membuss" "$@"
          APPRUN
          chmod +x "$APPDIR/AppRun"

      - name: Build AppImage
        run: |
          TAG="${GITHUB_REF#refs/tags/}"
          appimagetool Membuss.AppDir "Membuss-${TAG}-linux-amd64.AppImage"

      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: desktop-linux-appimage
          path: Membuss-*-linux-amd64.AppImage
          retention-days: 1
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: add Linux desktop AppImage build job"
```

---

### Task 5: Update release job to collect all artifacts

**Files:**
- Modify: `.github/workflows/release.yml` (the `release` job)

- [ ] **Step 1: Replace the release job**

Replace the existing `release` job with:

```yaml
  release:
    name: Create Release
    needs: [generate-notes, build-binaries, build-desktop-windows, build-desktop-linux]
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Download release notes
        uses: actions/download-artifact@v4
        with:
          name: release-notes

      - name: Download all build artifacts
        uses: actions/download-artifact@v4
        with:
          pattern: "{release-*,desktop-*}"
          merge-multiple: true
          path: artifacts

      - name: List artifacts
        run: find artifacts -type f

      - name: Create Release
        uses: softprops/action-gh-release@v2
        with:
          body_path: release-notes.md
          files: |
            artifacts/membuss-*.zip
            artifacts/membuss-*.tar.gz
            artifacts/Membuss-*-installer.exe
            artifacts/Membuss-*.AppImage
          draft: false
          prerelease: false
```

- [ ] **Step 2: Verify full workflow YAML syntax**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml'))"`
Expected: no output (valid YAML)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: update release job to collect all artifacts with commit-based notes"
```

---

### Task 6: Verify the complete workflow

- [ ] **Step 1: Read the final workflow file end-to-end**

Verify:
- `generate-notes` outputs `tag_name`
- `build-binaries` depends on `build-frontend` and `generate-notes`
- `build-desktop-windows` and `build-desktop-linux` both depend on `generate-notes`
- `release` depends on all four build jobs
- `release` uses `body_path: release-notes.md`
- All artifact patterns match what the build jobs upload

- [ ] **Step 2: Validate YAML one final time**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml'))"`
Expected: no output

- [ ] **Step 3: Test with a dry-run tag**

Create a test tag to verify the workflow triggers correctly:
```bash
git tag v0.1.3-rc.1
git push origin v0.1.3-rc.1
```
Then check the Actions tab on GitHub to verify all jobs run.

- [ ] **Step 4: Clean up test tag**

```bash
git tag -d v0.1.3-rc.1
git push origin :refs/tags/v0.1.3-rc.1
```
