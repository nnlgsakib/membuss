<script lang="ts">
	import { base } from '$app/paths';
	import { goto } from '$app/navigation';
	import { formatBytes } from '$lib/api';
	import Icon from '@iconify/svelte';

	let activeTab = $state<'file' | 'folder' | 'descriptor'>('file');
	let folderName = $state('');
	
	// File states
	let selectedFile = $state<File | null>(null);
	let selectedFiles = $state<FileList | null>(null);
	let descriptorFile = $state<File | null>(null);
	let descriptorStatus = $state<'idle' | 'importing' | 'fetching' | 'done' | 'error'>('idle');
	let descriptorError = $state('');
	let descriptorProgress = $state({ blocks: 0, total: 0, missing: 0 });
	
	// Upload Progress states
	let uploadActive = $state(false);
	let uploadStatusText = $state('Uploading...');
	let uploadPercent = $state(0);
	let loadedBytes = $state(0);
	let totalBytes = $state(0);
	let activeXhr = $state<XMLHttpRequest | null>(null);
	let uploadFilesCount = $state(0);
	let uploadFileList = $state<{ name: string; size: number; isFolder: boolean; path: string }[]>([]);
	let uploadPhase = $state<'uploading' | 'sealing' | 'done' | 'error'>('uploading');

	function handleFileChange(e: Event) {
		const target = e.target as HTMLInputElement;
		if (target.files && target.files.length > 0) {
			selectedFile = target.files[0];
		} else {
			selectedFile = null;
		}
	}

	function handleFolderChange(e: Event) {
		const target = e.target as HTMLInputElement;
		if (target.files && target.files.length > 0) {
			selectedFiles = target.files;
			
			// Auto populate folder name if empty
			if (!folderName) {
				const firstPath = target.files[0].webkitRelativePath || '';
				const parts = firstPath.split('/');
				if (parts.length > 1 && parts[0]) {
					folderName = parts[0];
				} else {
					folderName = 'Imported Folder';
				}
			}
		} else {
			selectedFiles = null;
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
			const formData = new FormData();
			formData.append('file', descriptorFile);
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

			if (descriptorStatus !== 'done') {
				descriptorStatus = 'error';
				descriptorError = 'Stream ended without completion';
			}
		} catch (err) {
			descriptorStatus = 'error';
			descriptorError = err instanceof Error ? err.message : 'Import failed';
		}
	}

	function cancelUpload() {
		if (activeXhr) {
			activeXhr.abort();
		}
		resetUploadState();
	}

	function resetUploadState() {
		uploadActive = false;
		uploadPercent = 0;
		loadedBytes = 0;
		totalBytes = 0;
		activeXhr = null;
		uploadFileList = [];
		uploadFilesCount = 0;
		uploadPhase = 'uploading';
	}

	function startUpload(formData: FormData, filesList: File[]) {
		resetUploadState();
		uploadActive = true;
		uploadFilesCount = filesList.length;
		
		totalBytes = filesList.reduce((acc, f) => acc + f.size, 0);
		
		uploadFileList = filesList.map(f => ({
			name: f.name,
			size: f.size,
			isFolder: !!f.webkitRelativePath && f.webkitRelativePath.includes('/'),
			path: f.webkitRelativePath || f.name
		}));

		uploadPhase = 'uploading';
		uploadStatusText = 'Uploading raw blocks...';

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
			// Upload completed, backend is now erasure coding, hashing, and sealing the Merkle tree
			uploadPhase = 'sealing';
			uploadStatusText = 'Erasure coding & sealing Merkle DAG...';
		});

		xhr.addEventListener('load', () => {
			if (xhr.status >= 200 && xhr.status < 300) {
				uploadPercent = 100;
				uploadPhase = 'done';
				uploadStatusText = 'Ingest complete!';

				// Redirect to resulting MID
				const finalUrl = xhr.responseURL || xhr.getResponseHeader('Location');
				setTimeout(() => {
					resetUploadState();
					if (finalUrl) {
						// Clean redirect to base explorer path
						const urlObj = new URL(finalUrl);
						goto(urlObj.pathname);
					} else {
						goto(`${base}/`);
					}
				}, 1000);
			} else {
				showError('Upload failed: ' + xhr.responseText);
			}
		});

		xhr.addEventListener('error', () => {
			showError('Network error occurred during transmission.');
		});

		function showError(msg: string) {
			uploadPhase = 'error';
			uploadStatusText = 'Failed';
			alert(msg);
			resetUploadState();
		}

		xhr.open('POST', `${base}/upload`);
		xhr.send(formData);
	}

	function handleFileSubmit(e: Event) {
		e.preventDefault();
		if (!selectedFile) return;

		const formData = new FormData();
		formData.append('file', selectedFile);
		
		startUpload(formData, [selectedFile]);
	}

	function handleFolderSubmit(e: Event) {
		e.preventDefault();
		if (!selectedFiles || selectedFiles.length === 0) return;

		const formData = new FormData();
		const filesArr: File[] = [];

		for (let i = 0; i < selectedFiles.length; i++) {
			const file = selectedFiles[i];
			filesArr.push(file);
			// Pass file and parallel relative path parameter
			formData.append('files', file);
			formData.append('paths', file.webkitRelativePath || file.name);
		}

		if (folderName.trim()) {
			formData.append('folder_name', folderName.trim());
		}

		startUpload(formData, filesArr);
	}
</script>

<div class="max-w-2xl w-full mx-auto flex flex-col gap-6">
	<!-- Page Header -->
	<div class="border-b border-slate-800 pb-4">
		<h1 class="text-2xl font-bold text-slate-50">Upload Content to Membuss</h1>
		<p class="text-xs text-slate-500 mt-1">Chunk, hash, erasure code, and index local files onto the network store</p>
	</div>

	<!-- Custom Tab Switcher -->
	<div class="flex border-b border-slate-800">
		<button
			onclick={() => activeTab = 'file'}
			class={`px-6 py-3 font-semibold text-sm transition-all border-b-2 -mb-[2px] ${
				activeTab === 'file'
					? 'border-cyan-500 text-cyan-400 font-bold'
					: 'border-transparent text-slate-500 hover:text-slate-350'
			}`}
		>
			Upload Single File
		</button>
		<button
			onclick={() => activeTab = 'folder'}
			class={`px-6 py-3 font-semibold text-sm transition-all border-b-2 -mb-[2px] ${
				activeTab === 'folder'
					? 'border-cyan-500 text-cyan-400 font-bold'
					: 'border-transparent text-slate-500 hover:text-slate-350'
			}`}
		>
			Upload Folder (MemFS)
		</button>
		<button
			onclick={() => activeTab = 'descriptor'}
			class={`px-6 py-3 font-semibold text-sm transition-all border-b-2 -mb-[2px] ${
				activeTab === 'descriptor'
					? 'border-cyan-500 text-cyan-400 font-bold'
					: 'border-transparent text-slate-500 hover:text-slate-350'
			}`}
		>
			Import .mbuss
		</button>
	</div>

	<!-- Content Forms -->
	<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl">
		{#if activeTab === 'file'}
			<!-- File Submission Form -->
			<form onsubmit={handleFileSubmit} class="flex flex-col gap-6">
				<div class="flex flex-col gap-2">
					<label class="text-xs font-mono text-slate-450">Choose File</label>
					<div class="border-2 border-dashed border-slate-800 hover:border-slate-700/80 rounded-xl p-8 flex flex-col items-center justify-center gap-3 relative transition-all group bg-slate-950/20">
						<span class="text-4xl group-hover:scale-110 transition-transform text-slate-400"><Icon icon="ph:file-text" /></span>
						<div class="text-sm font-semibold text-slate-300">
							{#if selectedFile}
								<span class="text-cyan-400">{selectedFile.name}</span>
								<span class="text-slate-500 text-xs block font-mono font-normal mt-1">({formatBytes(selectedFile.size)})</span>
							{:else}
								<span>Select a file to upload</span>
							{/if}
						</div>
						<p class="text-xs text-slate-650 max-w-xs text-center">
							Click to browse or drag your file directly inside this dropzone.
						</p>
						<input
							type="file"
							required
							disabled={uploadActive}
							onchange={handleFileChange}
							class="absolute inset-0 opacity-0 cursor-pointer w-full h-full"
						/>
					</div>
				</div>

				<button
					type="submit"
					disabled={!selectedFile || uploadActive}
					class="w-full py-3 bg-cyan-500 hover:bg-cyan-600 disabled:bg-slate-800 text-slate-950 disabled:text-slate-600 font-bold text-sm rounded-xl transition-all duration-300 active:scale-[0.98]"
				>
					Ingest File
				</button>
			</form>
		{:else if activeTab === 'folder'}
			<!-- Folder Submission Form -->
			<form onsubmit={handleFolderSubmit} class="flex flex-col gap-6">
				<div class="flex flex-col gap-2">
					<label class="text-xs font-mono text-slate-450">Choose Folder</label>
					<div class="border-2 border-dashed border-slate-800 hover:border-slate-700/80 rounded-xl p-8 flex flex-col items-center justify-center gap-3 relative transition-all group bg-slate-950/20">
						<span class="text-4xl group-hover:scale-110 transition-transform text-slate-400"><Icon icon="ph:folder-open" /></span>
						<div class="text-sm font-semibold text-slate-300">
							{#if selectedFiles && selectedFiles.length > 0}
								<span class="text-cyan-400">{selectedFiles.length} files selected</span>
								<span class="text-slate-500 text-xs block font-mono font-normal mt-1">
									(Root folder: "{folderName}")
								</span>
							{:else}
								<span>Select a directory tree to import</span>
							{/if}
						</div>
						<p class="text-xs text-slate-650 max-w-xs text-center">
							Click to browse folders. Directory nesting and metadata will be converted to a Merkle Link structure.
						</p>
						<input
							type="file"
							required
							webkitdirectory
							directory
							multiple
							disabled={uploadActive}
							onchange={handleFolderChange}
							class="absolute inset-0 opacity-0 cursor-pointer w-full h-full"
						/>
					</div>
				</div>

				<!-- Folder Metadata Name -->
				<div class="flex flex-col gap-1.5">
					<label for="folder-name-input" class="text-xs font-mono text-slate-450">
						Custom Root Name (Optional)
					</label>
					<input
						type="text"
						id="folder-name-input"
						bind:value={folderName}
						disabled={uploadActive}
						placeholder="Leave blank to use base folder name"
						class="w-full bg-slate-950/60 border border-slate-800 text-slate-200 text-sm px-4 py-2.5 rounded-lg focus:outline-none focus:border-cyan-500/80 focus:ring-1 focus:ring-cyan-500/20"
					/>
				</div>

				<button
					type="submit"
					disabled={!selectedFiles || uploadActive}
					class="w-full py-3 bg-cyan-500 hover:bg-cyan-600 disabled:bg-slate-800 text-slate-950 disabled:text-slate-600 font-bold text-sm rounded-xl transition-all duration-300 active:scale-[0.98]"
				>
					Ingest Directory
				</button>
			</form>
		{:else if activeTab === 'descriptor'}
			<form onsubmit={handleDescriptorSubmit} class="flex flex-col gap-6">
				<div class="flex flex-col gap-2">
					<label class="text-xs font-mono text-slate-450">Select .mbuss Descriptor File</label>
					<div class="border-2 border-dashed border-slate-800 hover:border-slate-700/80 rounded-xl p-8 flex flex-col items-center justify-center gap-3 relative transition-all group bg-slate-950/20">
						<span class="text-4xl group-hover:scale-110 transition-transform text-slate-400"><Icon icon="ph:file-arrow-down" /></span>
						<div class="text-sm font-semibold text-slate-300">
							{#if descriptorFile}
								<span class="text-cyan-400">{descriptorFile.name}</span>
								<span class="text-slate-500 text-xs block font-mono font-normal mt-1">({formatBytes(descriptorFile.size)})</span>
							{:else}
								<span>Select a .mbuss descriptor to import</span>
							{/if}
						</div>
						<p class="text-xs text-slate-650 max-w-xs text-center">
							Import a .mbuss descriptor file to verify and register content metadata on this node.
						</p>
						<input
							type="file"
							accept=".mbuss"
							required
							disabled={descriptorStatus === 'importing' || descriptorStatus === 'fetching'}
							onchange={handleDescriptorChange}
							class="absolute inset-0 opacity-0 cursor-pointer w-full h-full"
						/>
					</div>
				</div>

				{#if descriptorStatus === 'error'}
					<div class="bg-red-950/20 border border-red-800/40 text-red-400 px-4 py-3 rounded-lg text-xs font-mono">
						{descriptorError}
					</div>
				{/if}

				{#if descriptorStatus === 'done'}
					<div class="bg-emerald-950/20 border border-emerald-800/40 text-emerald-400 px-4 py-3 rounded-lg text-xs font-mono">
						Descriptor imported successfully! Redirecting...
					</div>
				{/if}

				{#if descriptorStatus === 'fetching'}
					<div class="flex flex-col gap-2">
						<div class="flex items-center justify-between text-[11px] font-mono text-slate-400">
							<span>Fetching from network...</span>
							<span>{descriptorProgress.blocks} / {descriptorProgress.total} blocks</span>
						</div>
						<div class="w-full h-2 rounded-full bg-slate-800 overflow-hidden">
							<div 
								class="h-full bg-cyan-500 transition-all duration-300"
								style={`width: ${descriptorProgress.total > 0 ? (descriptorProgress.blocks / descriptorProgress.total * 100) : 0}%`}
							></div>
						</div>
						<p class="text-[11px] text-slate-500">Downloading missing blocks from peers...</p>
					</div>
				{/if}

				<button
					type="submit"
					disabled={!descriptorFile || descriptorStatus === 'importing' || descriptorStatus === 'fetching'}
					class="w-full py-3 bg-cyan-500 hover:bg-cyan-600 disabled:bg-slate-800 text-slate-950 disabled:text-slate-600 font-bold text-sm rounded-xl transition-all duration-300 active:scale-[0.98] flex items-center justify-center gap-2"
				>
					{#if descriptorStatus === 'importing'}
						<div class="w-4 h-4 border-2 border-slate-950/30 border-t-slate-950 rounded-full animate-spin"></div>
						Verifying...
					{:else if descriptorStatus === 'fetching'}
						<div class="w-4 h-4 border-2 border-slate-950/30 border-t-slate-950 rounded-full animate-spin"></div>
						Fetching...
					{:else}
						<Icon icon="ph:download-simple" class="text-base" />
						Import Descriptor
					{/if}
				</button>
			</form>
		{/if}
	</div>
</div>

<!-- Upload Progress Overlay Widget -->
{#if uploadActive}
	<div class="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm flex items-center justify-center p-4">
		<div class="bg-slate-900 border border-slate-800 rounded-2xl w-full max-w-md shadow-2xl shadow-black/40 overflow-hidden flex flex-col">
			<!-- Header -->
			<div class="px-5 py-4 bg-slate-950/40 border-b border-slate-800/80 flex items-center justify-between">
				<div class="flex items-center gap-2 text-sm font-bold text-slate-300">
					{#if uploadPhase === 'uploading'}
						<div class="w-3.5 h-3.5 border-2 border-cyan-500/35 border-t-cyan-400 rounded-full animate-spin"></div>
					{:else if uploadPhase === 'sealing'}
						<div class="w-3.5 h-3.5 rounded-full bg-cyan-400 animate-ping"></div>
					{/if}
					<span>{uploadStatusText}</span>
				</div>
				{#if uploadPhase === 'uploading'}
					<button onclick={cancelUpload} class="text-xs text-slate-550 hover:text-red-400 border border-slate-800 hover:border-red-900/60 px-2 py-1 rounded transition-colors bg-slate-950/40">
						Cancel
					</button>
				{/if}
			</div>

			<!-- Body -->
			<div class="p-5 flex flex-col gap-4">
				<!-- Big percent indicator -->
				<div class="flex items-end justify-between font-mono">
					<span class="text-4xl font-bold text-cyan-400 leading-none">
						{uploadPercent}%
					</span>
					<span class="text-[11px] text-slate-500">
						{formatBytes(loadedBytes)} / {formatBytes(totalBytes)}
					</span>
				</div>

				<!-- Bar -->
				<div class="w-full h-2 rounded-full bg-slate-950 border border-slate-850 overflow-hidden">
					<div 
						class="h-full bg-cyan-500 transition-all duration-300"
						style={`width: ${uploadPercent}%`}
					></div>
				</div>

				<!-- Files list section -->
				<div class="flex flex-col gap-1.5 mt-2">
					<span class="text-[10px] font-mono text-slate-500 uppercase tracking-wide">
						Payload queue ({uploadFilesCount} files)
					</span>
					<div class="bg-slate-950/80 border border-slate-850 rounded-lg max-h-36 overflow-y-auto divide-y divide-slate-900/50 p-2 font-mono text-[10px] text-slate-500">
						{#each uploadFileList as file}
							<div class="py-1 px-1 flex justify-between gap-4">
								<span class="truncate text-slate-400 select-all" title={file.path}>{file.name}</span>
								<span class="shrink-0 text-slate-600">{formatBytes(file.size)}</span>
							</div>
						{/each}
					</div>
				</div>
			</div>
		</div>
	</div>
{/if}
