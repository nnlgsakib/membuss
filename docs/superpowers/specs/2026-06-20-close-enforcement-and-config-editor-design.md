# Design: Close Enforcement & YAML Config Editor

**Date:** 2026-06-20
**Status:** Approved
**Scope:** Desktop GUI (`desktop/`)

---

## 1. Close Enforcement

### Problem
Closing the app while the daemon node is running can corrupt BadgerDB data. Users need to be prompted to stop the node before the app exits.

### Design

**Go side (`app.go` — `beforeClose`):**
- When window close is triggered, check `dm.IsRunning()`
- If node is **not running**: allow close (return `false`)
- If node **is running**: emit Wails event `request-close` to frontend, block close (return `true`)

**Frontend side (`main.js`):**
- Register `wailsRuntime.EventsOn('request-close', ...)` listener
- On receive, show custom confirm modal:
  - Title: "Node Running"
  - Message: "The daemon node is still running. Stop the node and close the portal?"
  - Buttons: "Stop & Close" / "Cancel"
- "Stop & Close" → `await StopNode()` → `wailsRuntime.Quit()`
- "Cancel" → modal closes, app stays open

### Files changed
- `desktop/app.go` — modify `beforeClose` method
- `desktop/frontend/src/main.js` — add event listener + modal handler

---

## 2. YAML Config Editor

### Problem
The current config tab uses individual form inputs for each field. Users want to edit the raw `config.yaml` directly for full control.

### Design

**UI replacement:**
- Replace the existing form inputs in the config tab with a `<textarea>`
- Monospace font, dark background, full width
- Pre-filled with raw YAML content from `config.yaml`
- "Save & Restart" button below the textarea

**Save & Restart flow:**
1. User clicks "Save & Restart"
2. Frontend shows "Stopping node..." status
3. Call `StopNode()`
4. Call `SaveNodeConfigRaw(yamlContent)` — writes raw string to `config.yaml`
5. Frontend shows "Restarting node..." status
6. Call `StartNode()`
7. Show success toast
8. If YAML is invalid or save fails, show error modal

**Go methods (`app.go`):**
- `GetNodeConfigRaw() (string, error)` — reads `config.yaml` as raw string
- `SaveNodeConfigRaw(content string) error` — writes raw string to `config.yaml`

**Frontend (`main.js`):**
- `renderConfigTab` replaced to show textarea + save button
- Loading states during stop/save/restart cycle
- Error handling with custom alert modal

### Files changed
- `desktop/app.go` — add `GetNodeConfigRaw`, `SaveNodeConfigRaw` methods
- `desktop/frontend/src/main.js` — rewrite `renderConfigTab`

---

## Testing

1. **Close enforcement:** Start node → click X → verify modal appears → click Cancel → verify app stays open → click Stop & Close → verify node stops and app exits
2. **Config editor:** Navigate to config tab → verify YAML renders in textarea → edit a value → click Save & Restart → verify node restarts with new config
3. **Invalid YAML:** Enter malformed YAML → click Save → verify error modal appears, node stays running
