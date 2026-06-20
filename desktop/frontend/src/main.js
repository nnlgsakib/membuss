import './style.css';
import './app.css';

import {
  GetConfig,
  SaveConfig,
  SelectDirectory,
  InstallBinaries,
  GetNodeConfig,
  SaveNodeConfig,
  GetNodeConfigRaw,
  SaveNodeConfigRaw,
  WriteDefaultConfig,
  StartNode,
  StopNode,
  CheckNodeStatus,
  CheckExplorer,
  GetDaemonLogs,
  VerifyInstallation,
  ResetSetup
} from '../wailsjs/go/main/App';

import * as wailsRuntime from '../wailsjs/runtime/runtime';

// Application State
let appState = {
  config: null,
  activeTab: 'dashboard', // dashboard, explorer, config, logs
  installProgress: 0,
  installMessage: '',
  nodeStatus: {
    process_running: false,
    api_online: false,
    info: null,
    error: null
  },
  explorerOnline: false,
  nodeLogs: [],
  selectedDataPath: '',
  latestReleaseChecked: false
};

let logsPollerId = null;

// Initialize Application
async function init() {
  renderLoadingScreen();

  try {
    // Load config from Go backend
    appState.config = await GetConfig();
    
    if (appState.config.setup_complete) {
      // Verify local installation files
      const checks = await VerifyInstallation();
      if (checks.valid) {
        renderDashboardLayout();
        startStatusPolling();
      } else {
        renderBrokenInstallationScreen(checks);
      }
    } else {
      setTimeout(() => {
        renderWelcomeStep();
      }, 600); // Brief delay for transition
    }
  } catch (err) {
    console.error("Initialization error:", err);
    showGlobalError(err.message || err);
  }
}

// Listen for close confirmation from Go backend (registered once at startup)
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

// Global error helper
function showGlobalError(msg) {
  document.querySelector('#app').innerHTML = `
    <div class="wizard-container">
      <div class="wizard-card" style="border-color: var(--error)">
        <div class="brand-icon" style="background: var(--error)">!</div>
        <h1 class="wizard-title" style="color: var(--error)">Critical Error</h1>
        <p class="wizard-subtitle">${msg}</p>
        <button class="btn" onclick="window.location.reload()">Reload App</button>
      </div>
    </div>
  `;
}

  function renderLoadingScreen() {
    document.querySelector('#app').innerHTML = `
      <div class="wizard-container">
        <div class="wizard-card">
          <img src="./src/assets/icon.png" alt="Membuss" class="brand-icon" />
          <h1 class="wizard-title">Initializing Portal...</h1>
          <p class="wizard-subtitle" style="margin-bottom: 0;">Checking node installation and directories...</p>
        </div>
      </div>
    `;
  }

function renderBrokenInstallationScreen(checks) {
  const dataDir = appState.config.data_dir || 'N/A';
  document.querySelector('#app').innerHTML = `
    <div class="wizard-container">
      <div class="wizard-card" style="max-width: 600px; border-color: var(--error);">
        <div class="brand-icon" style="background: var(--error)">⚠️</div>
        <h1 class="wizard-title" style="color: var(--error)">Broken Installation</h1>
        <p class="wizard-subtitle">
          We found issues with the Membuss node installation in your configured directory:
          <br/><strong style="font-family: var(--font-mono); font-size: 11px; color: var(--text-main);">${dataDir}</strong>
        </p>

        <div style="background: var(--bg-main); border: 1px solid var(--border-color); border-radius: var(--border-radius); padding: 20px; text-align: left; margin-bottom: 30px; font-size: 13px;">
          <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
            <span>Data Folder Access:</span>
            <span style="color: ${checks.data_dir_ok ? 'var(--success)' : 'var(--error)'}; font-weight: bold;">
              ${checks.data_dir_ok ? '✓ OK' : '✗ MISSING / UNREADABLE'}
            </span>
          </div>
          <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
            <span>Daemon Engine Binary (membuss):</span>
            <span style="color: ${checks.daemon_bin_ok ? 'var(--success)' : 'var(--error)'}; font-weight: bold;">
              ${checks.daemon_bin_ok ? '✓ OK' : '✗ MISSING'}
            </span>
          </div>
          <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
            <span>CLI Binary Helper (membuss-cli):</span>
            <span style="color: ${checks.cli_bin_ok ? 'var(--success)' : 'var(--error)'}; font-weight: bold;">
              ${checks.cli_bin_ok ? '✓ OK' : '✗ MISSING'}
            </span>
          </div>
          <div style="display: flex; justify-content: space-between;">
            <span>Node Config (config.yaml):</span>
            <span style="color: ${checks.node_config_ok ? 'var(--success)' : 'var(--error)'}; font-weight: bold;">
              ${checks.node_config_ok ? '✓ OK' : '✗ MISSING'}
            </span>
          </div>
        </div>

        <div style="display: flex; justify-content: flex-end;">
          <button class="btn" id="btn-reset-setup" style="background: var(--error); box-shadow: 0 4px 12px var(--error-glow);">
            Reset Setup Wizard
          </button>
        </div>
      </div>
    </div>
  `;

  document.getElementById('btn-reset-setup').addEventListener('click', async () => {
    const ok = await showCustomConfirm("Reset Setup", "This will clear your previous folder settings and start the setup wizard again. Continue?");
    if (ok) {
      try {
        await ResetSetup();
        appState.config = await GetConfig();
        appState.selectedDataPath = '';
        renderWelcomeStep();
      } catch (err) {
        await showCustomAlert("Reset Failed", "Failed to reset: " + (err.message || err), "error");
      }
    }
  });
}

// ---------------------------------------------------------------------------
// WIZARD STEPS RENDERING
// ---------------------------------------------------------------------------

function renderWelcomeStep() {
  document.querySelector('#app').innerHTML = `
    <div class="wizard-container">
      <div class="wizard-card">
        <img src="./src/assets/icon.png" alt="Membuss" class="brand-icon" />
        <h1 class="wizard-title">Membuss Desktop Portal</h1>
        <p class="wizard-subtitle">
          Welcome to Membuss — a decentralized, content-addressed distributed storage and delivery network. 
          Let's get your local storage node initialized and connected to the network.
        </p>
        <button class="btn" id="btn-next-1">Get Started</button>
      </div>
    </div>
  `;
  document.getElementById('btn-next-1').addEventListener('click', renderPathSelectionStep);
}

function renderPathSelectionStep() {
  document.querySelector('#app').innerHTML = `
    <div class="wizard-container">
      <div class="wizard-card">
        <img src="./src/assets/icon.png" alt="Membuss" class="brand-icon" />
        <h1 class="wizard-title">Select Data Directory</h1>
        <p class="wizard-subtitle">
          Choose a directory in your filesystem where the Membuss node will store its binaries, 
          block data (BadgerDB), and configurations.
        </p>
        
        <div class="path-selector-box">
          <span class="path-text path-empty" id="selected-path-label">No folder selected</span>
          <button class="btn btn-secondary" id="btn-select-path" style="padding: 8px 16px;">Browse</button>
        </div>

        <div style="display: flex; justify-content: flex-end;">
          <button class="btn btn-secondary" id="btn-back-2">Back</button>
          <button class="btn" id="btn-next-2" disabled>Next Step</button>
        </div>
      </div>
    </div>
  `;

  const selectBtn = document.getElementById('btn-select-path');
  const nextBtn = document.getElementById('btn-next-2');
  const pathLabel = document.getElementById('selected-path-label');

  if (appState.selectedDataPath) {
    pathLabel.innerText = appState.selectedDataPath;
    pathLabel.classList.remove('path-empty');
    nextBtn.disabled = false;
  }

  selectBtn.addEventListener('click', async () => {
    try {
      const path = await SelectDirectory();
      if (path) {
        appState.selectedDataPath = path;
        pathLabel.innerText = path;
        pathLabel.classList.remove('path-empty');
        nextBtn.disabled = false;
      }
    } catch (err) {
      console.warn("Folder picker error or cancelled:", err);
    }
  });

  document.getElementById('btn-back-2').addEventListener('click', renderWelcomeStep);
  nextBtn.addEventListener('click', renderDownloadStep);
}

function renderDownloadStep() {
  document.querySelector('#app').innerHTML = `
    <div class="wizard-container">
      <div class="wizard-card" id="download-card">
        <img src="./src/assets/icon.png" alt="Membuss" class="brand-icon" />
        <h1 class="wizard-title" id="download-title">Installing Membuss Core</h1>
        <p class="wizard-subtitle" id="download-subtitle">
          Downloading and extracting the daemon binaries into your data directory.
        </p>
        
        <div class="progress-percent" id="download-percent">0%</div>
        <div class="progress-bar-container">
          <div class="progress-bar-fill" id="download-progress-bar"></div>
        </div>
        <p class="progress-message" id="download-msg">Initializing downloader...</p>
        
        <div style="display: flex; justify-content: flex-end; margin-top: 30px;" id="download-actions">
          <!-- Disabled during install -->
        </div>
      </div>
    </div>
  `;

  // Start binary installation
  InstallBinaries(appState.selectedDataPath)
    .catch(err => {
      console.error(err);
      showDownloadError(err.message || err);
    });

  // Listen for progress events from Wails
  wailsRuntime.EventsOn('install_progress', (data) => {
    const percent = data.percent;
    const msg = data.message;

    if (percent === -1) {
      // Error occurred
      showDownloadError(msg);
      return;
    }

    const bar = document.getElementById('download-progress-bar');
    const percentLabel = document.getElementById('download-percent');
    const msgLabel = document.getElementById('download-msg');

    if (bar) bar.style.width = `${percent}%`;
    if (percentLabel) percentLabel.innerText = `${percent}%`;
    if (msgLabel) msgLabel.innerText = msg;

    if (percent === 100) {
      // Complete
      setTimeout(() => {
        renderWizardConfigStep();
      }, 1000);
    }
  });
}

function showDownloadError(msg) {
  wailsRuntime.EventsOff('install_progress');
  const card = document.getElementById('download-card');
  if (!card) return;
  card.style.borderColor = 'var(--error)';
  
  document.getElementById('download-title').innerText = "Installation Failed";
  document.getElementById('download-title').style.color = "var(--error)";
  document.getElementById('download-percent').innerText = "ERROR";
  document.getElementById('download-percent').style.color = "var(--error)";
  document.getElementById('download-msg').innerHTML = `<span class="terminal-err">${msg}</span>`;

  const bar = document.getElementById('download-progress-bar');
  if (bar) {
    bar.style.width = '100%';
    bar.style.background = 'var(--error)';
  }

  document.getElementById('download-actions').innerHTML = `
    <button class="btn btn-secondary" onclick="window.location.reload()">Back to Select Folder</button>
  `;
}

async function renderWizardConfigStep() {
  document.querySelector('#app').innerHTML = `
    <div class="wizard-container">
      <div class="wizard-card" style="max-width: 640px;">
        <img src="./src/assets/icon.png" alt="Membuss" class="brand-icon" />
        <h1 class="wizard-title">Node Configuration</h1>
        <p class="wizard-subtitle">Set up your local node interface values. Default settings are ready to run.</p>
        
        <div class="form-grid">
          <div class="form-group">
            <label class="form-label">Listen Address (libp2p)</label>
            <input class="form-input form-input-mono" type="text" id="cfg-listen" value="/ip4/0.0.0.0/tcp/4001" />
          </div>
          
          <div class="form-group">
            <label class="form-label">Bootstrap Peer</label>
            <input class="form-input form-input-mono" type="text" id="cfg-bootstrap" value="/ip4/45.10.162.79/udp/4001/quic-v1/p2p/12D3KooWMNbuDSWaMw7evxzsp9CtaphofzxcEbHisWQQUmg7zfUx" />
          </div>
          
          <div class="toggle-group">
            <div class="toggle-label-container">
              <span class="toggle-title">Anchor Node Mode</span>
              <span class="toggle-desc">Backup and sync all network content chunks</span>
            </div>
            <label class="switch">
              <input type="checkbox" id="cfg-anchor" />
              <span class="slider"></span>
            </label>
          </div>

          <div class="toggle-group">
            <div class="toggle-label-container">
              <span class="toggle-title">Keep Alive in Background</span>
              <span class="toggle-desc">Minimizes to tray and continues running when GUI is closed</span>
            </div>
            <label class="switch">
              <input type="checkbox" id="cfg-keepalive" checked />
              <span class="slider"></span>
            </label>
          </div>
        </div>

        <div style="display: flex; justify-content: flex-end;">
          <button class="btn" id="btn-save-config">Finish & Start Node</button>
        </div>
      </div>
    </div>
  `;

  document.getElementById('btn-save-config').addEventListener('click', async () => {
    const listenAddr = document.getElementById('cfg-listen').value;
    const bootstrapPeer = document.getElementById('cfg-bootstrap').value;
    const anchorMode = document.getElementById('cfg-anchor').checked;
    const keepAlive = document.getElementById('cfg-keepalive').checked;

    try {
      // Generate full default config.yaml with all fields
      await WriteDefaultConfig();

      // Apply user overrides on top of defaults
      const yamlContent = await GetNodeConfigRaw();
      const lines = yamlContent.split('\n');
      const overridden = [];
      for (const line of lines) {
        if (line.startsWith('listen_addrs:')) {
          overridden.push(`listen_addrs:`);
          overridden.push(`    - ${listenAddr}`);
        } else if (line.startsWith('bootstrap_peers:')) {
          overridden.push(`bootstrap_peers:`);
          if (bootstrapPeer) overridden.push(`    - ${bootstrapPeer}`);
        } else if (line.startsWith('anchor_mode:')) {
          overridden.push(`anchor_mode: ${anchorMode}`);
        } else {
          overridden.push(line);
        }
      }
      await SaveNodeConfigRaw(overridden.join('\n'));

      // Save desktop-config.json properties
      const currentCfg = await GetConfig();
      currentCfg.setup_complete = true;
      currentCfg.keep_alive = keepAlive;
      await SaveConfig(currentCfg);

      appState.config = currentCfg;

      // Start the daemon process
      logMessage("Starting node daemon process...");
      await StartNode();
      
      // Load workspace dashboard
      renderDashboardLayout();
      startStatusPolling();
    } catch (err) {
      await showCustomAlert("Configuration Error", "Error saving configurations: " + (err.message || err), "error");
    }
  });
}

// ---------------------------------------------------------------------------
// DASHBOARD WORKSPACE RENDERING
// ---------------------------------------------------------------------------

function renderDashboardLayout() {
  document.querySelector('#app').innerHTML = `
    <div class="dashboard-layout">
      <!-- Sidebar -->
      <div class="sidebar">
        <div class="sidebar-brand">
          <img src="./src/assets/icon.png" alt="Membuss" class="brand-symbol" />
          <span class="brand-name">Membuss Desktop</span>
        </div>
        
        <ul class="nav-menu">
          <li class="nav-item active" id="nav-dash" data-tab="dashboard">📊 Dashboard</li>
          <li class="nav-item" id="nav-expl" data-tab="explorer">🌐 Explorer</li>
          <li class="nav-item" id="nav-conf" data-tab="config">⚙️ Node Settings</li>
          <li class="nav-item" id="nav-logs" data-tab="logs">📃 Activity Logs</li>
        </ul>
        
        <div class="sidebar-footer">
          <div class="status-pill">
            <div class="status-dot" id="status-dot"></div>
            <span id="status-text">OFFLINE</span>
          </div>
        </div>
      </div>
      
      <!-- Main Content Area -->
      <div class="main-panel">
        <div class="panel-header">
          <h2 class="panel-title" id="panel-title-text">Dashboard</h2>
          <div id="header-actions">
            <!-- Dynamic Actions -->
          </div>
        </div>
        
        <div class="panel-content" id="panel-body">
          <!-- Dynamic Content Rendered Here -->
        </div>
      </div>
    </div>
  `;

  // Attach nav event listeners
  document.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', (e) => {
      document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
      item.classList.add('active');
      const tab = item.getAttribute('data-tab');
      switchTab(tab);
    });
  });

  // Load initial tab
  switchTab('dashboard');
}

function switchTab(tab) {
  appState.activeTab = tab;
  const body = document.getElementById('panel-body');
  const titleText = document.getElementById('panel-title-text');
  const headerActions = document.getElementById('header-actions');

  headerActions.innerHTML = ''; // Reset actions

  // Clear any existing logs polling
  if (logsPollerId) {
    clearInterval(logsPollerId);
    logsPollerId = null;
  }

  if (tab === 'dashboard') {
    titleText.innerText = "Node Overview";
    renderDashboardTab(body, headerActions);
  } else if (tab === 'explorer') {
    titleText.innerText = "Network Web Explorer";
    renderExplorerTab(body);
  } else if (tab === 'config') {
    titleText.innerText = "Configuration Management";
    renderConfigTab(body);
  } else if (tab === 'logs') {
    titleText.innerText = "Console & Daemon Logs";
    renderLogsTab(body, headerActions);
    
    // Poll daemon logs every 1.5 seconds
    logsPollerId = setInterval(async () => {
      await fetchDaemonLogs();
    }, 1500);
    // run once immediately
    fetchDaemonLogs();
  }
}

// --- Tab 1: Dashboard --
function renderDashboardTab(container, headerActions) {
  const isRunning = appState.nodeStatus.process_running;

  // Add Start/Stop toggle button to header actions
  headerActions.innerHTML = `
    <button class="btn ${isRunning ? 'btn-secondary' : ''}" id="btn-toggle-node" style="padding: 8px 16px;">
      ${isRunning ? '🔴 Stop Node' : '🟢 Start Node'}
    </button>
  `;

  document.getElementById('btn-toggle-node').addEventListener('click', async () => {
    const btn = document.getElementById('btn-toggle-node');
    btn.disabled = true;
    try {
      if (appState.nodeStatus.process_running) {
        logMessage("Stopping node process...");
        await StopNode();
      } else {
        logMessage("Starting node process...");
        await StartNode();
      }
      setTimeout(async () => {
        await checkNodeHealth();
        switchTab('dashboard');
      }, 800);
    } catch (err) {
      await showCustomAlert("Process Error", "Process error: " + (err.message || err), "error");
      btn.disabled = false;
    }
  });

  const apiOnline = appState.nodeStatus.api_online || appState.nodeStatus.apiOnline;
  const rawInfo = appState.nodeStatus.info;

  let info = null;
  if (apiOnline && rawInfo) {
    if (rawInfo.data) {
      info = rawInfo.data;
    } else if (rawInfo.Data) {
      info = rawInfo.Data;
    } else if (rawInfo.peer_id || rawInfo.peerID || rawInfo.peerId || rawInfo.PeerID) {
      info = rawInfo;
    }
  }

  const peerID = info ? (info.peer_id || info.peerID || info.peerId || info.PeerID || 'N/A') : 'N/A';
  const ver = info ? (info.version || info.Version || 'N/A') : 'N/A';
  const bld = info ? (info.build || info.Build || 'N/A') : 'N/A';

  let addrs = [];
  if (info) {
    if (Array.isArray(info.addrs)) {
      addrs = info.addrs;
    } else if (Array.isArray(info.Addrs)) {
      addrs = info.Addrs;
    }
  }
  const addrsCount = addrs.length;

  container.innerHTML = `
    <div class="grid-3">
      <div class="stat-card">
        <span class="stat-label">Process Engine</span>
        <span class="stat-value" style="color: ${isRunning ? 'var(--success)' : 'var(--text-dark)'}">
          ${isRunning ? 'RUNNING' : 'STOPPED'}
        </span>
      </div>
      <div class="stat-card">
        <span class="stat-label">HTTP API Service</span>
        <span class="stat-value" style="color: ${apiOnline ? 'var(--success)' : 'var(--text-dark)'}">
          ${apiOnline ? 'ONLINE' : 'OFFLINE'}
        </span>
      </div>
      <div class="stat-card">
        <span class="stat-label">Local Gateway URL</span>
        <span class="stat-value-mono" style="margin-top: 10px;">
          <a href="http://${appState.config.gateway_addr}" target="_blank" style="color: var(--primary); text-decoration: none;">
            http://${appState.config.gateway_addr}
          </a>
        </span>
      </div>
    </div>

    <div class="grid-3">
      <div class="stat-card" style="grid-column: span 2;">
        <span class="stat-label">Node Peer Identity</span>
        <span class="stat-value-mono" title="${peerID}">${peerID}</span>
      </div>
      <div class="stat-card">
        <span class="stat-label">Engine Version</span>
        <span class="stat-value-mono">${ver || 'N/A'}</span>
      </div>
    </div>

    <div class="stat-card" style="margin-bottom: 20px;">
      <span class="stat-label">Connected Multiaddresses (${addrsCount})</span>
      <div style="font-family: var(--font-mono); font-size: 11px; max-height: 120px; overflow-y: auto; text-align: left; padding: 4px 0;">
        ${addrs.length > 0 ? 
          addrs.map(a => `<div style="margin-bottom: 4px; color: var(--text-muted);">${a}</div>`).join('') :
          '<div style="color: var(--text-dark);">No listener addresses found (Node offline)</div>'
        }
      </div>
    </div>

    <div class="stat-card">
      <span class="stat-label">Persistent Data Folder</span>
      <span class="stat-value-mono">${appState.config.data_dir}</span>
    </div>
  `;
}

// --- Tab 2: Explorer --
async function renderExplorerTab(container) {
  // Check if gateway's explorer endpoint is responsive
  const online = await CheckExplorer();
  appState.explorerOnline = online;

  if (online) {
    container.innerHTML = `
      <div class="explorer-container">
        <iframe src="http://${appState.config.gateway_addr}/explorer/" class="explorer-iframe"></iframe>
      </div>
    `;
  } else {
    container.innerHTML = `
      <div class="explorer-offline">
        <div class="offline-icon">🌐</div>
        <h3 style="font-size: 18px; margin-bottom: 8px;">Web Explorer Offline</h3>
        <p class="wizard-subtitle" style="max-width: 420px; margin-bottom: 24px;">
          The gateway server at http://${appState.config.gateway_addr} is not responding. 
          Please make sure the node is started and the gateway port is open.
        </p>
        <button class="btn" id="btn-retry-explorer">Check Connection Status</button>
      </div>
    `;
    document.getElementById('btn-retry-explorer').addEventListener('click', () => switchTab('explorer'));
  }
}

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

// --- Tab 4: Activity Logs --
function renderLogsTab(container, headerActions) {
  headerActions.innerHTML = `
    <button class="btn btn-secondary" id="btn-copy-logs" style="padding: 8px 16px; margin-right: 8px;">Copy Daemon Logs</button>
    <button class="btn btn-secondary" id="btn-clear-logs" style="padding: 8px 16px;">Clear Console</button>
  `;

  document.getElementById('btn-copy-logs').addEventListener('click', async () => {
    const daemonScreen = document.getElementById('daemon-terminal-screen');
    if (!daemonScreen) return;
    const text = daemonScreen.innerText;
    try {
      await navigator.clipboard.writeText(text);
      const btn = document.getElementById('btn-copy-logs');
      btn.innerText = "✓ Copied!";
      setTimeout(() => {
        const b = document.getElementById('btn-copy-logs');
        if (b) b.innerText = "Copy Daemon Logs";
      }, 2000);
    } catch (err) {
      await showCustomAlert("Copy Failed", "Failed to copy logs: " + err, "error");
    }
  });

  document.getElementById('btn-clear-logs').addEventListener('click', () => {
    appState.nodeLogs = [];
    renderLogsTab(container, headerActions);
  });

  const linesHtml = appState.nodeLogs.length > 0 ? 
    appState.nodeLogs.map(l => `<div class="terminal-line"><span class="text-dark">[${l.time}]</span> ${l.msg}</div>`).join('') :
    '<div class="terminal-line text-dark">Console idle. No events recorded yet.</div>';

  container.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 20px; height: 100%; text-align: left;">
      <div style="flex: 1; min-height: 120px; display: flex; flex-direction: column;">
        <span class="stat-label">Portal UI Event Logs</span>
        <div class="terminal-card" id="terminal-screen" style="flex-grow: 1; height: auto;">
          ${linesHtml}
        </div>
      </div>
      
      <div style="flex: 2; display: flex; flex-direction: column; min-height: 200px;">
        <span class="stat-label">Daemon Engine Standard Output (daemon.log)</span>
        <div class="terminal-card" id="daemon-terminal-screen" style="flex-grow: 1; height: auto; font-family: var(--font-mono); font-size: 11px; white-space: pre-wrap; background: #030508; color: #8ab4f8; overflow-y: auto;">
          Loading daemon output logs...
        </div>
      </div>
    </div>
  `;

  // Auto scroll portal screen to bottom
  const term = document.getElementById('terminal-screen');
  if (term) term.scrollTop = term.scrollHeight;
}

// Fetch raw daemon logs from file and display them
async function fetchDaemonLogs() {
  const daemonScreen = document.getElementById('daemon-terminal-screen');
  if (!daemonScreen) return;
  try {
    const logs = await GetDaemonLogs();
    if (daemonScreen.getAttribute('data-raw') !== logs) {
      daemonScreen.setAttribute('data-raw', logs);
      daemonScreen.innerText = logs;
      
      // Auto-scroll to bottom of logs
      daemonScreen.scrollTop = daemonScreen.scrollHeight;
    }
  } catch (err) {
    daemonScreen.innerText = "Error loading daemon logs: " + (err.message || err);
  }
}

// ---------------------------------------------------------------------------
// LOGGING AND POLLING LOGIC
// ---------------------------------------------------------------------------

function logMessage(msg) {
  const timestamp = new Date().toLocaleTimeString();
  appState.nodeLogs.push({ time: timestamp, msg: msg });
  
  // Keep logs list small
  if (appState.nodeLogs.length > 200) {
    appState.nodeLogs.shift();
  }

  // Update real-time if logs tab is open
  if (appState.activeTab === 'logs') {
    const term = document.getElementById('terminal-screen');
    if (term) {
      term.innerHTML += `<div class="terminal-line"><span class="text-dark">[${timestamp}]</span> ${msg}</div>`;
      term.scrollTop = term.scrollHeight;
    }
  }
}

// Background poller to check if Daemon is healthy
let statusPollerId = null;

function startStatusPolling() {
  if (statusPollerId) clearInterval(statusPollerId);
  
  // Immediate check
  checkNodeHealth();

  // Poll every 3 seconds
  statusPollerId = setInterval(checkNodeHealth, 3000);
}

async function checkNodeHealth() {
  try {
    const status = await CheckNodeStatus();
    const wasRunning = appState.nodeStatus.process_running;
    const wasOnline = appState.nodeStatus.api_online;

    appState.nodeStatus = status;

    // Log transitions
    if (status.process_running !== wasRunning) {
      logMessage(`Daemon process detected: ${status.process_running ? 'RUNNING' : 'STOPPED'}`);
    }
    if (status.api_online !== wasOnline) {
      logMessage(`Daemon API Connection: ${status.api_online ? 'CONNECTED' : 'DISCONNECTED'}`);
    }

    // Update UI components
    updateSidebarStatus();
    
    // If we are currently on the overview tab, re-render to update stat values
    if (appState.activeTab === 'dashboard') {
      const titleText = document.getElementById('panel-title-text');
      const body = document.getElementById('panel-body');
      const headerActions = document.getElementById('header-actions');
      if (titleText && titleText.innerText === "Node Overview" && body) {
        renderDashboardTab(body, headerActions);
      }
    }
  } catch (err) {
    console.error("Health poll error:", err);
  }
}

function updateSidebarStatus() {
  const dot = document.getElementById('status-dot');
  const text = document.getElementById('status-text');
  if (!dot || !text) return;

  const running = appState.nodeStatus.process_running || appState.nodeStatus.processRunning;
  const online = appState.nodeStatus.api_online || appState.nodeStatus.apiOnline;

  if (running && online) {
    dot.className = 'status-dot online';
    text.innerText = 'ONLINE';
    text.style.color = 'var(--success)';
  } else if (running) {
    dot.className = 'status-dot';
    dot.style.backgroundColor = 'var(--warning)';
    text.innerText = 'INITIALIZING';
    text.style.color = 'var(--warning)';
  } else {
    dot.className = 'status-dot';
    dot.style.backgroundColor = 'var(--text-dark)';
    text.innerText = 'OFFLINE';
    text.style.color = 'var(--text-muted)';
  }
}

// Custom Modal dialog helpers to replace browser alerts/confirms
function showCustomAlert(title, message, type = 'info') {
  const modal = document.createElement('div');
  modal.style.position = 'fixed';
  modal.style.top = '0';
  modal.style.left = '0';
  modal.style.width = '100vw';
  modal.style.height = '100vh';
  modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
  modal.style.display = 'flex';
  modal.style.alignItems = 'center';
  modal.style.justifyContent = 'center';
  modal.style.zIndex = '99999';
  modal.style.backdropFilter = 'blur(4px)';
  modal.style.animation = 'fadeIn 0.2s ease-out';

  let color = 'var(--primary)';
  let icon = 'ℹ️';
  if (type === 'error') { color = 'var(--error)'; icon = '✕'; }
  else if (type === 'success') { color = 'var(--success)'; icon = '✓'; }

  modal.innerHTML = `
    <div class="wizard-card" style="max-width: 440px; padding: 30px; border-color: ${color}; text-align: center; animation: modalIn 0.25s cubic-bezier(0.4, 0, 0.2, 1);">
      <div class="brand-icon" style="background: ${color}; width: 48px; height: 48px; font-size: 20px; margin-bottom: 16px; box-shadow: none;">${icon}</div>
      <h3 style="font-size: 18px; font-weight: 700; margin-bottom: 8px;">${title}</h3>
      <p style="font-size: 13px; color: var(--text-muted); margin-bottom: 24px; line-height: 1.5; text-align: center;">${message}</p>
      <button class="btn" id="custom-modal-ok" style="width: 100%;">OK</button>
    </div>
  `;

  document.body.appendChild(modal);
  
  return new Promise((resolve) => {
    document.getElementById('custom-modal-ok').addEventListener('click', () => {
      modal.remove();
      resolve();
    });
  });
}

function showCustomConfirm(title, message) {
  const modal = document.createElement('div');
  modal.style.position = 'fixed';
  modal.style.top = '0';
  modal.style.left = '0';
  modal.style.width = '100vw';
  modal.style.height = '100vh';
  modal.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
  modal.style.display = 'flex';
  modal.style.alignItems = 'center';
  modal.style.justifyContent = 'center';
  modal.style.zIndex = '99999';
  modal.style.backdropFilter = 'blur(4px)';
  modal.style.animation = 'fadeIn 0.2s ease-out';

  modal.innerHTML = `
    <div class="wizard-card" style="max-width: 440px; padding: 30px; border-color: var(--primary); text-align: center; animation: modalIn 0.25s cubic-bezier(0.4, 0, 0.2, 1);">
      <div class="brand-icon" style="background: var(--primary); width: 48px; height: 48px; font-size: 20px; margin-bottom: 16px; box-shadow: none;">❓</div>
      <h3 style="font-size: 18px; font-weight: 700; margin-bottom: 8px;">${title}</h3>
      <p style="font-size: 13px; color: var(--text-muted); margin-bottom: 24px; line-height: 1.5; text-align: center;">${message}</p>
      <div style="display: flex; gap: 12px; justify-content: flex-end;">
        <button class="btn btn-secondary" id="custom-modal-cancel" style="flex: 1; margin: 0; padding: 10px;">Cancel</button>
        <button class="btn" id="custom-modal-confirm" style="flex: 1; padding: 10px;">Confirm</button>
      </div>
    </div>
  `;

  document.body.appendChild(modal);

  return new Promise((resolve) => {
    document.getElementById('custom-modal-confirm').addEventListener('click', () => {
      modal.remove();
      resolve(true);
    });
    document.getElementById('custom-modal-cancel').addEventListener('click', () => {
      modal.remove();
      resolve(false);
    });
  });
}

// Start Initialization
document.addEventListener('DOMContentLoaded', init);
