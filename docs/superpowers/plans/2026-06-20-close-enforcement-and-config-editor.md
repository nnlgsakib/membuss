# Close Enforcement & YAML Config Editor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add close enforcement (prompt to stop node before closing) and a raw YAML config editor with auto-restart on save.

**Architecture:** Two Go methods added to `app.go` for raw YAML I/O. `beforeClose` modified to emit a Wails event when the node is running. Frontend listens for the event and shows a custom modal. Config tab rewritten to show a textarea with raw YAML and a Save & Restart button.

**Tech Stack:** Go, Wails v2, JavaScript (vanilla), chi middleware

---

## File Map

| File | Responsibility |
|------|---------------|
| `desktop/app.go` | `beforeClose` modification, `GetNodeConfigRaw`, `SaveNodeConfigRaw` |
| `desktop/frontend/src/main.js` | `request-close` listener, config tab rewrite with textarea |

---

### Task 1: Add raw YAML read/write methods to Go

**Files:**
- Modify: `desktop/app.go:113-127`

- [ ] **Step 1: Add `GetNodeConfigRaw` method**

Add this method after the existing `GetNodeConfig` method (around line 119) in `desktop/app.go`:

```go
// GetNodeConfigRaw returns the raw YAML content of config.yaml.
func (a *App) GetNodeConfigRaw() (string, error) {
	if a.config.DataDir == "" {
		return "", errors.New("no data directory configured")
	}
	path := filepath.Join(a.config.DataDir, "config.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}
```

- [ ] **Step 2: Add `SaveNodeConfigRaw` method**

Add this method after the existing `SaveNodeConfig` method (around line 127) in `desktop/app.go`:

```go
// SaveNodeConfigRaw writes raw YAML content to config.yaml.
func (a *App) SaveNodeConfigRaw(content string) error {
	if a.config.DataDir == "" {
		return errors.New("no data directory configured")
	}
	path := filepath.Join(a.config.DataDir, "config.yaml")
	return os.WriteFile(path, []byte(content), 0644)
}
```

- [ ] **Step 3: Verify compilation**

Run: `go build ./desktop/...`
Expected: builds without errors

- [ ] **Step 4: Commit**

```bash
git add desktop/app.go
git commit -m "feat: add GetNodeConfigRaw and SaveNodeConfigRaw methods"
```

---

### Task 2: Modify `beforeClose` to emit close event

**Files:**
- Modify: `desktop/app.go:256-270`

- [ ] **Step 1: Replace the `beforeClose` method**

Replace the existing `beforeClose` method in `desktop/app.go` with:

```go
// beforeClose is triggered when the window is closed.
// If the daemon is running, emit an event to the frontend to confirm close.
func (a *App) beforeClose(ctx context.Context) bool {
	if a.config.KeepAlive && a.config.SetupComplete && a.daemonManager.IsRunning() {
		// Emit event to frontend to handle close confirmation
		wailsRuntime.EventsEmit(ctx, "request-close")
		return true // Block close, let frontend handle it
	}

	// If not keeping alive or node not running, stop daemon and allow close
	_ = a.daemonManager.Stop()
	return false // Allow app termination
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./desktop/...`
Expected: builds without errors

- [ ] **Step 3: Commit**

```bash
git add desktop/app.go
git commit -m "feat: emit request-close event when node is running"
```

---

### Task 3: Add frontend close confirmation listener

**Files:**
- Modify: `desktop/frontend/src/main.js` (near `init()` function, around line 42)

- [ ] **Step 1: Add Wails runtime import**

Check that `wailsRuntime` is already imported at the top of `main.js`. It should already be present:

```javascript
import * as wailsRuntime from '../wailsjs/runtime/runtime';
```

If not present, add it after the existing imports.

- [ ] **Step 2: Add `request-close` event listener in `init()`**

Add this block inside the `init()` function, after the `if (appState.config.setup_complete)` check succeeds and before `renderDashboardLayout()`:

```javascript
// Listen for close confirmation from Go backend
wailsRuntime.EventsOn('request-close', async () => {
  const ok = await showCustomConfirm(
    "Node Running",
    "The daemon node is still running. Stop the node and close the portal?"
  );
  if (ok) {
    try {
      await StopNode();
    } catch (e) {
      // Ignore stop errors, proceed with close
    }
    wailsRuntime.Quit();
  }
});
```

- [ ] **Step 3: Verify the frontend compiles**

Run: `cd desktop/frontend && npm run build` (or check Vite dev server output)
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add desktop/frontend/src/main.js
git commit -m "feat: add request-close event listener with stop-and-close flow"
```

---

### Task 4: Rewrite config tab with raw YAML textarea

**Files:**
- Modify: `desktop/frontend/src/main.js:642-748` (the `renderConfigTab` function)

- [ ] **Step 1: Replace `renderConfigTab` function**

Replace the entire `renderConfigTab` function (from line 642 to line 748) with:

```javascript
// --- Tab 3: Config Settings (Raw YAML Editor) ---
async function renderConfigTab(container) {
  container.innerHTML = `<div class="progress-message">Loading config.yaml...</div>`;

  try {
    const yamlContent = await GetNodeConfigRaw();

    container.innerHTML = `
      <div style="background: var(--bg-surface); padding: 30px; border-radius: 12px; border: 1px solid var(--border-color); text-align: left;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
          <h3 style="font-size: 16px; font-weight: 600; margin: 0;">config.yaml</h3>
          <span style="font-size: 12px; color: var(--text-muted);">Edit raw YAML configuration</span>
        </div>

        <textarea
          id="config-yaml-editor"
          style="
            width: 100%;
            min-height: 320px;
            background: #1a1b26;
            color: #a9b1d6;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 16px;
            font-family: var(--font-mono);
            font-size: 13px;
            line-height: 1.6;
            resize: vertical;
            tab-size: 2;
            outline: none;
          "
          spellcheck="false"
        >${yamlContent}</textarea>

        <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 16px;">
          <span id="config-save-status" style="font-size: 12px; color: var(--text-muted);"></span>
          <button class="btn" id="btn-save-yaml" style="min-width: 160px;">Save & Restart</button>
        </div>
      </div>
    `;

    // Handle tab key in textarea
    const editor = document.getElementById('config-yaml-editor');
    editor.addEventListener('keydown', (e) => {
      if (e.key === 'Tab') {
        e.preventDefault();
        const start = editor.selectionStart;
        const end = editor.selectionEnd;
        editor.value = editor.value.substring(0, start) + '  ' + editor.value.substring(end);
        editor.selectionStart = editor.selectionEnd = start + 2;
      }
    });

    // Save & Restart handler
    document.getElementById('btn-save-yaml').addEventListener('click', async () => {
      const btn = document.getElementById('btn-save-yaml');
      const status = document.getElementById('config-save-status');
      const newYaml = editor.value;

      btn.disabled = true;
      btn.innerText = 'Stopping Node...';
      status.innerText = 'Stopping daemon process...';
      status.style.color = 'var(--warning)';

      try {
        // 1. Stop node
        await StopNode();

        // 2. Save config
        btn.innerText = 'Saving Config...';
        status.innerText = 'Writing config.yaml...';
        await SaveNodeConfigRaw(newYaml);

        // 3. Restart node
        btn.innerText = 'Restarting...';
        status.innerText = 'Starting daemon process...';
        status.style.color = 'var(--primary)';
        await StartNode();

        // 4. Success
        btn.innerText = 'Save & Restart';
        status.innerText = 'Config saved and node restarted successfully.';
        status.style.color = 'var(--success)';
        logMessage("Config.yaml updated and node restarted.");
      } catch (err) {
        btn.innerText = 'Save & Restart';
        status.innerText = '';
        await showCustomAlert("Error", "Failed to save config or restart node: " + (err.message || err), "error");
      } finally {
        btn.disabled = false;
      }
    });

  } catch (err) {
    container.innerHTML = `<div class="terminal-err">Error loading config: ${err.message || err}</div>`;
  }
}
```

- [ ] **Step 2: Verify `GetNodeConfigRaw`, `SaveNodeConfigRaw`, `StopNode`, `StartNode` are in the Wails bindings import**

Check the import block at the top of `main.js` (around line 1-18). Ensure these are listed:

```javascript
import {
  GetConfig,
  SaveConfig,
  SelectDirectory,
  InstallBinaries,
  GetNodeConfig,
  SaveNodeConfig,
  GetNodeConfigRaw,
  SaveNodeConfigRaw,
  StartNode,
  StopNode,
  CheckNodeStatus,
  CheckExplorer,
  GetDaemonLogs,
  VerifyInstallation,
  ResetSetup
} from '../wailsjs/go/main/App';
```

Add any missing ones.

- [ ] **Step 3: Verify the frontend compiles**

Run: `cd desktop/frontend && npm run build`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add desktop/frontend/src/main.js
git commit -m "feat: replace config tab with raw YAML editor and auto-restart"
```

---

### Task 5: End-to-end verification

- [ ] **Step 1: Start the app in dev mode**

Run: `wails dev` from the `desktop/` directory
Expected: app launches without errors

- [ ] **Step 2: Test close enforcement**
1. Start the node from the dashboard
2. Click the window close (X) button
3. Verify: custom modal appears with "Node Running" title
4. Click "Cancel" — verify app stays open
5. Click the close button again
6. Click "Stop & Close" — verify node stops and app exits

- [ ] **Step 3: Test config editor**
1. Navigate to the Config tab
2. Verify: raw YAML textarea appears with config.yaml content
3. Edit a value (e.g., change `reprovide_interval`)
4. Click "Save & Restart"
5. Verify: status shows "Stopping Node..." → "Saving Config..." → "Restarting..." → success
6. Verify: the changed value persists after reload

- [ ] **Step 4: Test invalid YAML**
1. Type invalid YAML (e.g., unmatched brackets)
2. Click "Save & Restart"
3. Verify: error modal appears, node keeps running with old config

- [ ] **Step 5: Final commit if needed**

```bash
git add -A
git commit -m "feat: close enforcement and YAML config editor complete"
```
