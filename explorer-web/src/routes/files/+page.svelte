<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { apiFetch, formatBytes } from '$lib/api';
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import Icon from '@iconify/svelte';

	interface StoredMID {
		MID: string;
		Name: string;
		Sealed: boolean;
		Size: number;
		MimeType: string;
	}

	interface IndexData {
		AllFiles: StoredMID[];
	}

	// Local file cache derived from the index response
	interface LocalFile {
		mid: string;
		name: string;
		size: number;
		sealed: boolean;
		mime: string;
		type: 'file' | 'dir';
	}

	let fileList = $state<LocalFile[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);

	// Search & Filters
	let filterStatus = $state<'all' | 'sealed' | 'unsealed'>('all');
	let searchQuery = $state('');
	let filteredFiles = $derived.by(() => {
		return fileList.filter(f => {
			const matchesStatus = 
				filterStatus === 'all' || 
				(filterStatus === 'sealed' && f.sealed) || 
				(filterStatus === 'unsealed' && !f.sealed);
			const matchesSearch = 
				!searchQuery || 
				f.name.toLowerCase().includes(searchQuery.toLowerCase()) || 
				f.mid.toLowerCase().includes(searchQuery.toLowerCase());
			return matchesStatus && matchesSearch;
		});
	});

	// Upload States
	let activeUploadTab = $state<'file' | 'folder' | 'descriptor'>('file');
	let folderName = $state('');
	let selectedFile = $state<File | null>(null);
	let selectedFiles = $state<FileList | null>(null);
	let descriptorFile = $state<File | null>(null);
	let descriptorStatus = $state<'idle' | 'importing' | 'fetching' | 'done' | 'error'>('idle');
	let descriptorError = $state('');
	let descriptorProgress = $state({ blocks: 0, total: 0, missing: 0 });
	
	// Upload Progress
	let uploadPercent = $state(0);
	let uploadActive = $state(false);
	let uploadStatusText = $state('');
	let loadedBytes = $state(0);
	let totalBytes = $state(0);
	let uploadFileList = $state<{ name: string; size: number }[]>([]);
	let uploadPhase = $state<'uploading' | 'sealing' | 'done'>('uploading');
	let activeXhr = $state<XMLHttpRequest | null>(null);

	// Network Fetch (Resolve MID) State
	let fetchMIDInput = $state('');
	let resolvingMIDs = $state<{ 
		mid: string; 
		statusText: string; 
		percent: number; 
		blocksResolved: number; 
		blocksTotal: number;
		eventSource: EventSource | null;
	}[]>([]);

	// Share Copy Toast
	let copiedId = $state<string | null>(null);

	// Load file list from the index endpoint (all metadata included)
	async function loadFiles() {
		try {
			const indexRes: IndexData = await apiFetch('/');
			const allFiles = indexRes.AllFiles || [];
			
			const mapped: LocalFile[] = allFiles.map((item) => ({
				mid: item.MID,
				name: item.Name || 'Unnamed Record',
				sealed: item.Sealed,
				size: item.Size || 0,
				mime: item.MimeType || 'application/octet-stream',
				type: item.MimeType === 'inode/directory' ? 'dir' : 'file'
			}));

			fileList = mapped;
			loading = false;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to query file store';
			loading = false;
		}
	}

	// Trigger Seal / Unseal operations directly from the list
	async function toggleSeal(file: LocalFile) {
		const action = file.sealed ? 'unseal' : 'seal';
		try {
			const res = await fetch(`${base}/mid/${file.mid}/${action}`, {
				method: 'POST'
			});
			if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`);
			
			file.sealed = !file.sealed;
			loadFiles();
		} catch (err) {
			alert(`Action failed: ${err instanceof Error ? err.message : err}`);
		}
	}

	async function deleteFile(file: LocalFile) {
		if (!confirm(`Are you sure you want to delete "${file.name}" and all its blocks recursively from this node? This cannot be undone.`)) {
			return;
		}
		try {
			const res = await fetch(`${base}/mid/${file.mid}/delete`, {
				method: 'POST'
			});
			if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`);
			
			// Remove from local list immediately
			fileList = fileList.filter(f => f.mid !== file.mid);
		} catch (err) {
			alert(`Delete failed: ${err instanceof Error ? err.message : err}`);
		}
	}

	// Copy gateway link to share
	function shareFile(file: LocalFile) {
		let shareUrl = '';
		if (file.type === 'dir' && window.location.hostname === 'localhost') {
			const portStr = window.location.port ? `:${window.location.port}` : '';
			shareUrl = `${window.location.protocol}//${file.mid}.localhost${portStr}/`;
		} else {
			const gateBase = window.location.origin;
			shareUrl = `${gateBase}/mem/${file.mid}${file.type === 'dir' ? '/' : ''}`;
		}
		navigator.clipboard.writeText(shareUrl).then(() => {
			copiedId = file.mid;
			setTimeout(() => {
				if (copiedId === file.mid) copiedId = null;
			}, 2000);
		});
	}

	// Trigger network fetch of a remote MID
	async function fetchMID(e: Event) {
		e.preventDefault();
		const midVal = fetchMIDInput.trim().replace('/mem/', '');
		if (!midVal) return;

		// Check if already in fileList
		if (fileList.some(f => f.mid === midVal)) {
			alert('This Content ID is already present in your local store.');
			fetchMIDInput = '';
			return;
		}

		// Check if already resolving
		if (resolvingMIDs.some(r => r.mid === midVal)) {
			alert('This Content ID is already actively resolving from the DHT.');
			fetchMIDInput = '';
			return;
		}

		// Trigger background resolving by registering an EventSource stream
		const url = `${base}/mid/${midVal}/resolve-stream`;
		const es = new EventSource(url);
		
		const session = {
			mid: midVal,
			statusText: 'Connecting to DHT...',
			percent: 0,
			blocksResolved: 0,
			blocksTotal: 0,
			eventSource: es
		};

		resolvingMIDs = [...resolvingMIDs, session];
		fetchMIDInput = '';

		es.onmessage = (ev) => {
			const d = JSON.parse(ev.data);
			const idx = resolvingMIDs.findIndex(r => r.mid === midVal);
			if (idx === -1) return;

			if (d.error) {
				resolvingMIDs[idx].statusText = 'Error: ' + d.error;
				es.close();
				setTimeout(() => removeResolving(midVal), 5000);
				return;
			}

			if (d.done) {
				es.close();
				resolvingMIDs[idx].statusText = 'Pinning to local store...';
				resolvingMIDs[idx].percent = 100;
				// Seal (pin) the fetched content so it appears in the file list
				fetch(`${base}/mid/${midVal}/seal`, { method: 'POST' })
					.finally(() => {
						loadFiles();
						resolvingMIDs[idx].statusText = 'Resolved!';
						setTimeout(() => removeResolving(midVal), 2000);
					});
				return;
			}

			if (d.state === 'connecting') {
				resolvingMIDs[idx].statusText = 'Locating providers...';
			}

			if (d.total > 0) {
				resolvingMIDs[idx].statusText = 'Downloading pieces...';
				resolvingMIDs[idx].blocksTotal = d.total;
				resolvingMIDs[idx].blocksResolved = d.blocks;
				resolvingMIDs[idx].percent = Math.round((d.blocks / d.total) * 100);
			}
		};

		es.onerror = () => {
			const idx = resolvingMIDs.findIndex(r => r.mid === midVal);
			if (idx !== -1) resolvingMIDs[idx].statusText = 'Lost connection, retrying...';
		};
	}

	function removeResolving(mid: string) {
		const s = resolvingMIDs.find(r => r.mid === mid);
		if (s && s.eventSource) s.eventSource.close();
		resolvingMIDs = resolvingMIDs.filter(r => r.mid !== mid);
	}

	// File Ingestion Upload Handlers
	function handleUpload(files: File[], customFolderName?: string) {
		uploadActive = true;
		uploadPercent = 0;
		loadedBytes = 0;
		totalBytes = files.reduce((acc, f) => acc + f.size, 0);
		uploadFileList = files.map(f => ({ name: f.name, size: f.size }));
		uploadPhase = 'uploading';
		uploadStatusText = 'Uploading raw blocks...';

		const formData = new FormData();
		if (activeUploadTab === 'file') {
			formData.append('file', files[0]);
		} else {
			for (let i = 0; i < files.length; i++) {
				formData.append('files', files[i]);
				formData.append('paths', files[i].webkitRelativePath || files[i].name);
			}
			if (customFolderName) {
				formData.append('folder_name', customFolderName);
			}
		}

		const xhr = new XMLHttpRequest();
		activeXhr = xhr;

		xhr.upload.addEventListener('progress', (e) => {
			if (e.lengthComputable) {
				loadedBytes = e.loaded;
				totalBytes = e.total;
				uploadPercent = Math.round((e.loaded / e.total) * 100);
			}
		});

		xhr.upload.addEventListener('load', () => {
			uploadPhase = 'sealing';
			uploadStatusText = 'Erasure coding & sealing Merkle DAG...';
		});

		xhr.addEventListener('load', () => {
			if (xhr.status >= 200 && xhr.status < 300) {
				uploadPercent = 100;
				uploadPhase = 'done';
				uploadStatusText = 'Ingest complete!';
				
				setTimeout(() => {
					uploadActive = false;
					selectedFile = null;
					selectedFiles = null;
					folderName = '';
					loadFiles();
				}, 1000);
			} else {
				alert('Upload failed: ' + xhr.responseText);
				uploadActive = false;
			}
		});

		xhr.addEventListener('error', () => {
			alert('Network error occurred.');
			uploadActive = false;
		});

		xhr.open('POST', `${base}/upload`);
		xhr.send(formData);
	}

	function cancelUpload() {
		if (activeXhr) {
			activeXhr.abort();
			activeXhr = null;
		}
		uploadActive = false;
	}

	function triggerUploadForm(e: Event) {
		e.preventDefault();
		if (activeUploadTab === 'file' && selectedFile) {
			handleUpload([selectedFile]);
		} else if (activeUploadTab === 'folder' && selectedFiles && selectedFiles.length > 0) {
			const filesArr: File[] = [];
			for (let i = 0; i < selectedFiles.length; i++) {
				filesArr.push(selectedFiles[i]);
			}
			handleUpload(filesArr, folderName);
		}
	}

	function handleFileChange(e: Event) {
		const target = e.target as HTMLInputElement;
		if (target.files && target.files.length > 0) selectedFile = target.files[0];
	}

	function handleFolderChange(e: Event) {
		const target = e.target as HTMLInputElement;
		if (target.files && target.files.length > 0) {
			selectedFiles = target.files;
			if (!folderName) {
				const firstPath = target.files[0].webkitRelativePath || '';
				folderName = firstPath.split('/')[0] || 'Imported Folder';
			}
		}
	}

	function handleDescriptorChange(e: Event) {
		const target = e.target as HTMLInputElement;
		if (target.files && target.files.length > 0) {
			descriptorFile = target.files[0];
			descriptorStatus = 'idle';
			descriptorError = '';
		} else {
			descriptorFile = null;
		}
	}

	async function handleDescriptorSubmit(e: Event) {
		e.preventDefault();
		if (!descriptorFile) return;
		descriptorStatus = 'importing';
		descriptorError = '';
		descriptorProgress = { blocks: 0, total: 0, missing: 0 };

		try {
			// First, upload the .mbuss file to the streaming endpoint
			const formData = new FormData();
			formData.append('file', descriptorFile);

			// Use fetch to POST, then read the SSE stream from the response
			const res = await fetch(`${base}/descriptor/import-stream`, { method: 'POST', body: formData });
			if (!res.ok) {
				const txt = await res.text();
				throw new Error(txt || `HTTP ${res.status}`);
			}

			const reader = res.body?.getReader();
			if (!reader) throw new Error('No response stream');

			const decoder = new TextDecoder();
			let buffer = '';

			while (true) {
				const { done, value } = await reader.read();
				if (done) break;

				buffer += decoder.decode(value, { stream: true });
				const lines = buffer.split('\n');
				buffer = lines.pop() || '';

				for (const line of lines) {
					if (!line.startsWith('data: ')) continue;
					const jsonStr = line.slice(6).trim();
					if (!jsonStr) continue;

					try {
						const ev = JSON.parse(jsonStr);
						if (ev.error) {
							descriptorStatus = 'error';
							descriptorError = ev.error;
							return;
						}
						if (ev.state === 'fetching') {
							descriptorStatus = 'fetching';
							descriptorProgress = { blocks: 0, total: ev.total || 0, missing: ev.missing || 0 };
						}
						if (ev.state === 'downloading') {
							descriptorStatus = 'fetching';
							descriptorProgress = { blocks: ev.blocks || 0, total: ev.total || 0, missing: descriptorProgress.missing };
						}
						if (ev.done && ev.mid) {
							descriptorStatus = 'done';
							setTimeout(() => {
								goto(`${base}/mid/${ev.mid}`);
							}, 500);
							return;
						}
					} catch {
						// skip malformed lines
					}
				}
			}

			// If we get here without a done event, something went wrong
			if (descriptorStatus !== 'done') {
				descriptorStatus = 'error';
				descriptorError = 'Stream ended without completion';
			}
		} catch (err) {
			descriptorStatus = 'error';
			descriptorError = err instanceof Error ? err.message : 'Import failed';
		}
	}

	onMount(() => {
		loadFiles();
	});

	onDestroy(() => {
		if (activeXhr) activeXhr.abort();
		resolvingMIDs.forEach(r => r.eventSource && r.eventSource.close());
	});
</script>

<div class="flex flex-col gap-6">
	<!-- Page Header -->
	<div class="border-b border-slate-800/80 pb-4">
		<h1 class="text-2xl font-bold text-slate-100">Local File System</h1>
		<p class="text-xs text-slate-500 mt-1">Manage files, seal/pin redundancy parameters, and fetch Merkle DAGs from the network</p>
	</div>

	<!-- Top split actions layout -->
	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-stretch">
		
		<!-- Action Panel 1: Upload (merged uploader) -->
		<div class="bg-slate-900 border border-slate-800/80 rounded-xl p-5 lg:col-span-7 flex flex-col gap-4 relative overflow-hidden">
			<div class="flex border-b border-slate-700/50">
				<button 
					onclick={() => activeUploadTab = 'file'}
					class={`pb-2 px-3 text-xs font-mono font-bold tracking-wider uppercase border-b-2 -mb-[2px] transition-all ${
						activeUploadTab === 'file' ? 'border-cyan-500 text-cyan-400' : 'border-transparent text-slate-500'
					}`}
				>
					File Upload
				</button>
			<button 
				onclick={() => activeUploadTab = 'folder'}
				class={`pb-2 px-3 text-xs font-mono font-bold tracking-wider uppercase border-b-2 -mb-[2px] transition-all ${
					activeUploadTab === 'folder' ? 'border-cyan-500 text-cyan-400' : 'border-transparent text-slate-500'
				}`}
			>
				Directory Upload
			</button>
			<button 
				onclick={() => activeUploadTab = 'descriptor'}
				class={`pb-2 px-3 text-xs font-mono font-bold tracking-wider uppercase border-b-2 -mb-[2px] transition-all ${
					activeUploadTab === 'descriptor' ? 'border-cyan-500 text-cyan-400' : 'border-transparent text-slate-500'
				}`}
			>
				Import .mbuss
			</button>
			</div>

			{#if activeUploadTab === 'descriptor'}
				<form onsubmit={handleDescriptorSubmit} class="flex flex-col gap-4 flex-grow justify-between">
					<div class="group relative border border-slate-700/50 hover:border-slate-700/50 rounded-lg p-5 flex flex-col items-center text-center gap-2 select-none cursor-pointer bg-slate-950/30 py-7">
						<Icon icon="ph:file-arrow-down" class="text-4xl text-slate-500 group-hover:scale-110 transition-transform" />
						<span class="text-xs font-bold text-slate-300">
							{descriptorFile ? descriptorFile.name : 'Select a .mbuss descriptor'}
						</span>
						{#if descriptorFile}
							<span class="text-[10px] text-slate-500 font-mono">({formatBytes(descriptorFile.size)})</span>
						{/if}
						<input type="file" accept=".mbuss" required disabled={descriptorStatus === 'importing'} onchange={handleDescriptorChange} class="absolute inset-0 opacity-0 cursor-pointer w-full h-full" />
					</div>
					{#if descriptorStatus === 'error'}
						<div class="bg-red-950/20 border border-red-800/40 text-red-400 px-4 py-3 rounded-lg text-xs font-mono">
							{descriptorError}
						</div>
					{/if}
					{#if descriptorStatus === 'done'}
						<div class="bg-emerald-950/20 border border-emerald-800/40 text-emerald-400 px-4 py-3 rounded-lg text-xs font-mono">
							Imported! Redirecting...
						</div>
					{/if}
					{#if descriptorStatus === 'fetching'}
						<div class="flex flex-col gap-2">
							<div class="flex items-center justify-between text-[10px] font-mono text-slate-400">
								<span>Fetching from network...</span>
								<span>{descriptorProgress.blocks} / {descriptorProgress.total} blocks</span>
							</div>
							<div class="w-full h-1.5 rounded-full bg-slate-800 overflow-hidden">
								<div 
									class="h-full bg-cyan-500 transition-all duration-300"
									style={`width: ${descriptorProgress.total > 0 ? (descriptorProgress.blocks / descriptorProgress.total * 100) : 0}%`}
								></div>
							</div>
							<p class="text-[10px] text-slate-500">Downloading missing blocks from peers...</p>
						</div>
					{/if}
					<button 
						type="submit" 
						disabled={!descriptorFile || descriptorStatus === 'importing' || descriptorStatus === 'fetching'}
						class="w-full py-2.5 bg-cyan-500 hover:bg-cyan-400 disabled:bg-slate-800 text-slate-950 disabled:text-slate-500 text-xs font-bold rounded-lg transition-all duration-200 active:scale-[0.98] flex items-center justify-center gap-2"
					>
						{#if descriptorStatus === 'importing'}
							<div class="w-3.5 h-3.5 border-2 border-slate-950/30 border-t-slate-950 rounded-full animate-spin"></div>
							Verifying...
						{:else if descriptorStatus === 'fetching'}
							<div class="w-3.5 h-3.5 border-2 border-slate-950/30 border-t-slate-950 rounded-full animate-spin"></div>
							Fetching...
						{:else}
							<Icon icon="ph:download-simple" class="text-sm" />
							Import Descriptor
						{/if}
					</button>
				</form>
			{:else}
				<form onsubmit={triggerUploadForm} class="flex flex-col gap-4 flex-grow justify-between">
					{#if activeUploadTab === 'file'}
						<div class="group relative border border-slate-700/50 hover:border-slate-700/50 rounded-lg p-5 flex flex-col items-center text-center gap-2 select-none cursor-pointer bg-slate-950/30 py-7">
							<Icon icon="ph:upload-simple" class="text-4xl text-slate-500 group-hover:scale-110 transition-transform" />
							<span class="text-xs font-bold text-slate-300">
								{selectedFile ? selectedFile.name : 'Select or drop a file'}
							</span>
							{#if selectedFile}
								<span class="text-[10px] text-slate-500 font-mono">({formatBytes(selectedFile.size)})</span>
							{/if}
							<input type="file" required onchange={handleFileChange} class="absolute inset-0 opacity-0 cursor-pointer w-full h-full" />
						</div>
					{:else}
						<div class="group relative border border-slate-700/50 hover:border-slate-700/50 rounded-lg p-5 flex flex-col items-center text-center gap-2 select-none cursor-pointer bg-slate-950/30 py-4">
							<Icon icon="ph:folder-open" class="text-4xl text-slate-500 group-hover:scale-110 transition-transform" />
							<span class="text-xs font-bold text-slate-300">
								{selectedFiles && selectedFiles.length > 0 ? `${selectedFiles.length} files selected` : 'Select a directory to import'}
							</span>
							<input type="file" required webkitdirectory directory multiple onchange={handleFolderChange} class="absolute inset-0 opacity-0 cursor-pointer w-full h-full" />
						</div>
						<input 
							type="text" 
							bind:value={folderName} 
							placeholder="Custom root directory name (optional)" 
							class="w-full bg-slate-950/60 border border-slate-700/50 text-xs px-3.5 py-2.5 rounded-lg focus:outline-none focus:border-cyan-500" 
						/>
					{/if}

					<button 
						type="submit" 
						disabled={(activeUploadTab === 'file' ? !selectedFile : !selectedFiles) || uploadActive}
						class="w-full py-2.5 bg-cyan-500 hover:bg-cyan-400 disabled:bg-slate-800 text-slate-950 disabled:text-slate-500 text-xs font-bold rounded-lg transition-all duration-200 active:scale-[0.98]"
					>
						{uploadActive ? 'Processing Ingest...' : 'Ingest to Network'}
					</button>
				</form>
			{/if}
		</div>

		<!-- Action Panel 2: Fetch CID/MID from Swarm DHT -->
		<div class="bg-slate-900 border border-slate-800/80 rounded-xl p-5 lg:col-span-5 flex flex-col gap-4">
			<h3 class="font-bold text-xs text-slate-400 font-mono uppercase tracking-wider border-b border-slate-700/50 pb-2">
				Swarm Ingest (Fetch CID)
			</h3>
			<p class="text-[11px] text-slate-500 leading-relaxed font-sans">
				Import content by entering its Content Identifier (MID). Membuss will query Kademlia routing tables and resolve blocks via P2P Memex stream sessions.
			</p>
			
			<form onsubmit={fetchMID} class="flex flex-col gap-4 mt-auto">
				<input
					type="text"
					bind:value={fetchMIDInput}
					required
					placeholder="Enter mem1z... multihash address"
					class="w-full bg-slate-950/60 border border-slate-700/50 text-xs px-3.5 py-2.5 rounded-lg focus:outline-none focus:border-cyan-500/80 focus:ring-1 focus:ring-cyan-500/20 font-mono"
				/>
				<button 
					type="submit"
					class="w-full py-2.5 bg-slate-800 hover:bg-slate-600 border border-slate-700/50 text-slate-200 text-xs font-bold rounded-lg transition-all duration-200 active:scale-[0.98]"
				>
					Resolve & Fetch Content
				</button>
			</form>
		</div>
	</div>

	<!-- Active resolving background tasks list -->
	{#if resolvingMIDs.length > 0}
		<section class="bg-slate-900 border border-slate-800/80 rounded-xl p-5 flex flex-col gap-4">
			<h3 class="font-bold text-xs text-slate-400 font-mono uppercase tracking-wider border-b border-slate-700/50 pb-1">
				Active DHT Resolving Queue
			</h3>
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#each resolvingMIDs as res}
					<div class="bg-slate-950/60 border border-slate-700/50 rounded-lg p-3 flex flex-col gap-2 font-mono text-[10px] relative">
						<button 
							onclick={() => removeResolving(res.mid)}
							class="absolute top-2 right-3 text-slate-600 hover:text-slate-300 text-xs"
						>
							✕
						</button>
						<div class="flex flex-col">
							<span class="text-slate-500 uppercase text-[8px]">Fetching Target</span>
							<span class="text-slate-200 font-bold break-all select-all">{res.mid}</span>
						</div>
						<div class="flex items-center justify-between border-t border-slate-800/40 pt-2 text-[9px] text-slate-500">
							<span>{res.statusText}</span>
							<span class="font-bold text-cyan-400">{res.percent}%</span>
						</div>
						<div class="w-full h-1 bg-slate-900 rounded-full overflow-hidden">
							<div class="h-full bg-cyan-400 transition-all duration-300" style={`width: ${res.percent}%`}></div>
						</div>
					</div>
				{/each}
			</div>
		</section>
	{/if}

	<!-- File List Toolbar (Search + Filters) -->
	<section class="bg-slate-900 border border-slate-800/80 rounded-xl p-5 flex flex-col gap-5">
		<div class="flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-4 border-b border-slate-700/50 pb-4">
			<!-- Tab filters -->
			<div class="flex items-center gap-1.5 p-1 bg-slate-950/80 border border-slate-700/50 rounded-lg">
				<button 
					onclick={() => filterStatus = 'all'} 
					class={`px-3 py-1.5 rounded-md text-[10px] font-bold font-mono tracking-wider uppercase transition-colors ${
						filterStatus === 'all' ? 'bg-slate-800/60 text-cyan-400 border border-slate-800/80' : 'text-slate-500 hover:text-slate-300'
					}`}
				>
					All Files
				</button>
				<button 
					onclick={() => filterStatus = 'sealed'} 
					class={`px-3 py-1.5 rounded-md text-[10px] font-bold font-mono tracking-wider uppercase transition-colors ${
						filterStatus === 'sealed' ? 'bg-slate-800/60 text-cyan-400 border border-slate-800/80' : 'text-slate-500 hover:text-slate-300'
					}`}
				>
					Pinned / Sealed
				</button>
				<button 
					onclick={() => filterStatus = 'unsealed'} 
					class={`px-3 py-1.5 rounded-md text-[10px] font-bold font-mono tracking-wider uppercase transition-colors ${
						filterStatus === 'unsealed' ? 'bg-slate-800/60 text-cyan-400 border border-slate-800/80' : 'text-slate-500 hover:text-slate-300'
					}`}
				>
					Unpinned
				</button>
			</div>

			<!-- Search filter input -->
			<div class="relative w-full sm:w-72">
				<input
					type="text"
					bind:value={searchQuery}
					placeholder="Filter by name or MID..."
					class="w-full bg-slate-950/60 border border-slate-700/50 text-xs px-3.5 py-2 rounded-lg focus:outline-none focus:border-cyan-500"
				/>
				{#if searchQuery}
					<button onclick={() => searchQuery = ''} class="absolute right-3 top-2 text-slate-500 hover:text-slate-300 text-xs font-bold">✕</button>
				{/if}
			</div>
		</div>

		<!-- File List Table -->
		{#if loading}
			<div class="space-y-3 animate-pulse py-4">
				<div class="h-8 bg-slate-800/60 rounded w-full"></div>
				<div class="h-8 bg-slate-800/60 rounded w-full"></div>
				<div class="h-8 bg-slate-800/60 rounded w-full"></div>
			</div>
		{:else if filteredFiles && filteredFiles.length > 0}
			<div class="overflow-x-auto">
				<table class="w-full text-left border-collapse text-xs">
					<thead>
						<tr class="border-b border-slate-800/80 text-slate-500 font-mono text-[10px] uppercase bg-slate-950/20">
							<th class="py-2.5 px-4 font-semibold">Name</th>
							<th class="py-2.5 px-4 font-semibold w-1/3">Content Address (MID)</th>
							<th class="py-2.5 px-4 font-semibold w-24">Size</th>
							<th class="py-2.5 px-4 font-semibold w-24 text-center">Status</th>
							<th class="py-2.5 px-4 font-semibold text-right">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-800/60">
						{#each filteredFiles as file (file.mid)}
							<tr class="hover:bg-slate-700/30 transition-colors group">
								<!-- Icon + Name -->
								<td class="py-3 px-4">
									<div class="flex items-center gap-2">
										{#if file.type === 'dir'}
											<Icon icon="ph:folder" class="w-4 h-4 text-slate-400" />
										{:else}
											<Icon icon="ph:file-text" class="w-4 h-4 text-slate-400" />
										{/if}
										<a 
											href={`${base}/mid/${file.mid}`} 
											class="font-bold text-slate-200 hover:text-cyan-400 hover:underline break-all truncate max-w-[200px]"
											title={file.name}
										>
											{file.name}
										</a>
									</div>
								</td>

								<!-- MID -->
								<td class="py-3 px-4 font-mono text-slate-500">
									<a href={`${base}/mid/${file.mid}`} class="hover:text-cyan-400 hover:underline">
										{file.mid}
									</a>
								</td>

								<!-- Size -->
								<td class="py-3 px-4 font-mono text-slate-400">
									{file.type === 'dir' ? '—' : formatBytes(file.size)}
								</td>

								<!-- Status Badges -->
								<td class="py-3 px-4 text-center">
									{#if file.sealed}
										<span class="px-2 py-0.5 rounded text-[9px] font-bold font-mono bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
											PINNED
										</span>
									{:else}
										<span class="px-2 py-0.5 rounded text-[9px] font-bold font-mono bg-slate-800 text-slate-500 border border-slate-700/50">
											UNPINNED
										</span>
									{/if}
								</td>

								<!-- In-line actions -->
								<td class="py-3 px-4 text-right">
									<div class="flex items-center justify-end gap-3 text-[11px]">
										<!-- Pin/Unpin (Seal/Unseal) -->
										<button 
											onclick={() => toggleSeal(file)}
											class={`font-bold hover:underline ${
												file.sealed ? 'text-amber-500 hover:text-amber-400' : 'text-emerald-500 hover:text-emerald-400'
											}`}
										>
											{file.sealed ? 'Unpin' : 'Pin'}
										</button>

										<!-- Share gateway link -->
										<button 
											onclick={() => shareFile(file)}
											class="text-cyan-500 hover:text-cyan-400 font-bold hover:underline"
										>
											{copiedId === file.mid ? 'Copied ✓' : 'Share'}
										</button>

										<!-- View details -->
										<a 
											href={`${base}/mid/${file.mid}`} 
											class="text-slate-400 hover:text-slate-200 font-bold hover:underline"
										>
											Inspect
										</a>

										<!-- Delete recursively -->
										<button 
											onclick={() => deleteFile(file)}
											class="text-red-500 hover:text-red-400 font-bold hover:underline"
										>
											Delete
										</button>
									</div>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{:else}
			<div class="py-16 text-center flex flex-col items-center justify-center gap-3">
				<Icon icon="ph:files" class="text-4xl text-slate-600" />
				<div class="text-sm font-semibold text-slate-400">No Files Match Current Filters</div>
				<p class="text-xs text-slate-500 max-w-xs leading-relaxed">
					Refine your search parameters or check other tabs to locate your Content IDs.
				</p>
			</div>
		{/if}
	</section>
</div>

<!-- Upload Progress Widget Overlay -->
{#if uploadActive}
	<div class="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm flex items-center justify-center p-4">
		<div class="bg-slate-900 border border-slate-800/80 rounded-xl w-full max-w-md shadow-2xl overflow-hidden flex flex-col">
			<!-- Header -->
			<div class="px-5 py-4 bg-slate-950/40 border-b border-slate-800/80 flex items-center justify-between">
				<div class="flex items-center gap-2 text-xs font-bold font-mono text-slate-300">
					{#if uploadPhase === 'uploading'}
						<div class="w-3 h-3 border border-cyan-500/35 border-t-cyan-400 rounded-full animate-spin"></div>
					{:else if uploadPhase === 'sealing'}
						<div class="w-3 h-3 rounded-full bg-cyan-400 animate-ping"></div>
					{/if}
					<span>{uploadStatusText}</span>
				</div>
				{#if uploadPhase === 'uploading'}
					<button onclick={cancelUpload} class="text-[10px] text-slate-500 hover:text-red-400 border border-slate-800/80 px-2 py-0.5 rounded bg-slate-950/40 font-mono">
						Cancel
					</button>
				{/if}
			</div>

			<!-- Body -->
			<div class="p-5 flex flex-col gap-4 font-mono text-xs">
				<!-- Big percent indicator -->
				<div class="flex items-end justify-between">
					<span class="text-3xl font-black text-cyan-400 leading-none">
						{uploadPercent}%
					</span>
					<span class="text-[10px] text-slate-500">
						{formatBytes(loadedBytes)} / {formatBytes(totalBytes)}
					</span>
				</div>

				<!-- Bar -->
				<div class="w-full h-1.5 rounded-full bg-slate-950 border border-slate-700/50 overflow-hidden">
					<div 
						class="h-full bg-gradient-to-r from-cyan-500 to-blue-500 transition-all duration-300"
						style={`width: ${uploadPercent}%`}
					></div>
				</div>

				<!-- Files list section -->
				<div class="flex flex-col gap-1.5 mt-2">
					<span class="text-[9px] text-slate-500 uppercase tracking-wide">
						Uploading {uploadFileList.length} items
					</span>
					<div class="bg-slate-950/80 border border-slate-700/50 rounded-lg max-h-24 overflow-y-auto divide-y divide-slate-800/40 p-2 text-[9px] text-slate-500">
						{#each uploadFileList as file}
							<div class="py-1 px-1 flex justify-between gap-4">
								<span class="truncate text-slate-400 select-all">{file.name}</span>
								<span class="shrink-0 text-slate-600">{formatBytes(file.size)}</span>
							</div>
						{/each}
					</div>
				</div>
			</div>
		</div>
	</div>
{/if}
