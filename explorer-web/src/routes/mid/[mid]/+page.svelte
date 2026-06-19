<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { page } from '$app/state';
	import { base } from '$app/paths';
	import { apiFetch, formatBytes } from '$lib/api';
	import DagNode from '$lib/components/DagNode.svelte';

	interface MemFSEntry {
		name: string;
		mid: string;
		type: string;
		size: number;
	}

	interface MIDData {
		Title: string;
		MID: string;
		NotFound: boolean;
		Name: string;
		MimeType: string;
		Sealers: number;
		AnchorSealers: number;
		Providers: string[] | null;
		Size: number;
		Blocks: number;
		Sealed: boolean;
		Codec: number;
		ContentType: string;
		DataShards: number;
		ParityShards: number;
		TotalShards: number;
		Health: string;
		HealthLabel: string;
		MemFSType: string;
		MemFSEntries: MemFSEntry[] | null;
		SymlinkTarget: string;
		ResolveStatus: number;
		ResolveMessage: string;
	}

	let midVal = $derived(page.params.mid);
	let data = $state<MIDData | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let copiedMID = $state(false);
	
	// Actions states
	let renameValue = $state('');
	let isRenaming = $state(false);

	// Tabs
	let activeTab = $state<'info' | 'dag'>('info');

	// Active Resolver States (SSE stream)
	let resolverActive = $state(false);
	let statusBadgeText = $state('Connecting');
	let statStatusText = $state('Connecting to network DHT...');
	let statPeers = $state(0);
	let statBlocksResolved = $state(0);
	let statBlocksTotal = $state(0);
	let statAvailability = $state('0%');
	let activeProviders = $state<string[]>([]);
	let eventSource = $state<EventSource | null>(null);
	let pieceGrid = $state<('queued' | 'scanning' | 'checked' | 'downloaded' | 'finished')[]>([]);

	// Initialize the piece visualizer placeholder
	const MAX_BOXES = 120;
	function initPieceGrid(total: number) {
		const size = total > 0 ? Math.min(MAX_BOXES, total) : 40;
		pieceGrid = Array(size).fill('queued');
	}

	function startResolutionStream() {
		if (eventSource) return;

		resolverActive = true;
		initPieceGrid(0);
		
		const url = `${base}/mid/${midVal}/resolve-stream`;
		const es = new EventSource(url);
		eventSource = es;

		let hasStartedScan = false;
		let checkComplete = false;
		let checkTimer: number;

		es.onmessage = (ev) => {
			const d = JSON.parse(ev.data);
			if (d.error) {
				statStatusText = 'Error: ' + d.error;
				statusBadgeText = 'Failed';
				es.close();
				clearInterval(checkTimer);
				return;
			}
			if (d.done) {
				es.close();
				clearInterval(checkTimer);
				pieceGrid = Array(pieceGrid.length).fill('finished');
				statusBadgeText = 'Complete';
				statStatusText = 'Assembly Complete!';
				
				// Reload page to get finalized content
				setTimeout(() => {
					loadMID();
				}, 1000);
				return;
			}

			if (d.providers) {
				activeProviders = d.providers;
				statPeers = d.providers.length;
			}

			if (d.state === 'connecting') {
				statStatusText = 'Connecting to network DHT...';
				statusBadgeText = 'Connecting';
			}

			if (d.total > 0) {
				statBlocksTotal = d.total;
				statBlocksResolved = d.blocks;

				if (!hasStartedScan) {
					hasStartedScan = true;
					initPieceGrid(d.total);
					statusBadgeText = 'Verifying';
					statStatusText = 'Checking network piece availability...';
					
					// Fast sequential scanning animation
					let currentIndex = 0;
					const displayCount = pieceGrid.length;
					const delay = Math.max(5, Math.min(40, Math.floor(600 / displayCount)));
					
					checkTimer = setInterval(() => {
						if (currentIndex >= displayCount) {
							clearInterval(checkTimer);
							checkComplete = true;
							statAvailability = '100% available';
							statStatusText = 'Downloading pieces...';
							statusBadgeText = 'Downloading';
							updateGridState(d.total, d.blocks, checkComplete);
							return;
						}
						pieceGrid[currentIndex] = 'scanning';
						const capturedIndex = currentIndex;
						setTimeout(() => {
							if (pieceGrid[capturedIndex] === 'scanning') {
								pieceGrid[capturedIndex] = 'checked';
							}
						}, delay * 2);
						currentIndex++;
						statAvailability = `${Math.round(currentIndex * 100 / displayCount)}%`;
					}, delay) as unknown as number;
				} else {
					updateGridState(d.total, d.blocks, checkComplete);
				}
			}
		};

		es.onerror = () => {
			statStatusText = 'Connection lost. Retrying...';
		};
	}

	function updateGridState(total: number, resolved: number, checkDone: boolean) {
		const displayCount = pieceGrid.length;
		for (let i = 0; i < displayCount; i++) {
			const startIdx = Math.floor(i * total / displayCount);
			const endIdx = Math.floor((i + 1) * total / displayCount);

			if (resolved >= endIdx) {
				pieceGrid[i] = 'downloaded';
			} else if (resolved > startIdx) {
				pieceGrid[i] = 'scanning';
			} else if (checkDone) {
				pieceGrid[i] = 'checked';
			}
		}
	}

	async function loadMID() {
		loading = true;
		error = null;
		resolverActive = false;
		
		if (eventSource) {
			eventSource.close();
			eventSource = null;
		}

		try {
			const res = await apiFetch(`/mid/${midVal}`);
			data = res;
			if (data) {
				renameValue = data.Name || '';
				// If not found locally but we have active providers list, run the SSE solver
				if (data.NotFound && data.Providers && data.Providers.length > 0) {
					startResolutionStream();
				}
			}
			loading = false;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to query Content ID metadata';
			loading = false;
		}
	}

	function copyToClipboard() {
		navigator.clipboard.writeText(midVal).then(() => {
			copiedMID = true;
			setTimeout(() => copiedMID = false, 1500);
		});
	}

	async function runAction(action: 'seal' | 'unseal') {
		try {
			loading = true;
			const res = await fetch(`${base}/mid/${midVal}/${action}`, {
				method: 'POST'
			});
			if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`);
			loadMID();
		} catch (err) {
			alert(`Action failed: ${err instanceof Error ? err.message : err}`);
			loading = false;
		}
	}

	async function renameContent(e: Event) {
		e.preventDefault();
		const name = renameValue.trim();
		if (!name) return;

		isRenaming = true;
		try {
			const formData = new FormData();
			formData.append('name', name);
			const res = await fetch(`${base}/mid/${midVal}/rename`, {
				method: 'POST',
				body: formData
			});
			if (!res.ok) throw new Error(await res.text() || `HTTP ${res.status}`);
			
			// Reload
			loadMID();
		} catch (err) {
			alert(`Rename failed: ${err instanceof Error ? err.message : err}`);
		} finally {
			isRenaming = false;
		}
	}

	// Trigger loadMID whenever midVal changes (Svelte reactive statement)
	$effect(() => {
		if (midVal) {
			loadMID();
		}
	});

	onDestroy(() => {
		if (eventSource) eventSource.close();
	});
</script>

<div class="flex flex-col gap-6">
	<!-- Page Header -->
	<div class="border-b border-zinc-800 pb-4 flex flex-wrap items-center justify-between gap-4">
		<div>
			<div class="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded bg-zinc-900 border border-zinc-800 text-[10px] text-zinc-400 font-mono tracking-wider uppercase">
				Content Address
			</div>
			<h1 class="text-xl md:text-2xl font-black text-zinc-50 mt-1 select-all break-all flex items-center gap-2">
				{#if data && data.Name}
					<span class="text-zinc-100">{data.Name}</span>
					<span class="text-zinc-500 font-normal font-mono text-sm">({midVal.slice(0,8)}...)</span>
				{:else}
					<span>{midVal}</span>
				{/if}
			</h1>
		</div>

		{#if data && !data.NotFound}
			<!-- Rename Action Panel -->
			<form onsubmit={renameContent} class="flex items-center gap-2">
				<input
					type="text"
					bind:value={renameValue}
					required
					disabled={isRenaming}
					placeholder="Rename content alias"
					class="bg-zinc-950/60 border border-zinc-800 text-zinc-200 text-xs px-3 py-1.5 rounded-lg focus:outline-none focus:border-cyan-500/80 focus:ring-1 focus:ring-cyan-500/20"
				/>
				<button 
					type="submit" 
					disabled={isRenaming || (data && renameValue.trim() === data.Name)}
					class="px-3.5 py-1.5 rounded-lg bg-zinc-800 hover:bg-zinc-700 disabled:opacity-50 text-xs font-bold text-zinc-200 border border-zinc-750 transition-colors"
				>
					{isRenaming ? 'Saving...' : 'Rename'}
				</button>
			</form>
		{/if}
	</div>

	<!-- Copy Row -->
	<div class="bg-zinc-900/60 border border-zinc-800 rounded-xl px-4 py-3 flex items-center justify-between gap-4">
		<code class="font-mono text-xs text-zinc-300 break-all select-all">{midVal}</code>
		<button 
			onclick={copyToClipboard}
			class="shrink-0 px-3 py-1.5 rounded bg-zinc-800 hover:bg-zinc-700 text-xs font-bold text-zinc-200 border border-zinc-750 transition-colors"
		>
			{copiedMID ? 'Copied ✓' : 'Copy MID 📋'}
		</button>
	</div>

	{#if loading && !data && !resolverActive}
		<div class="space-y-6 animate-pulse">
			<div class="h-44 bg-zinc-900 rounded-lg"></div>
			<div class="h-28 bg-zinc-900 rounded-lg"></div>
		</div>
	{:else if error}
		<div class="bg-red-950/20 border border-red-800/40 text-red-400 p-4 rounded-xl text-xs font-mono">
			{error}
		</div>
	{:else if data}
		<!-- RESOLVER MODE (if not found but resolving in background) -->
		{#if resolverActive}
			<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 flex flex-col gap-6 shadow-xl relative overflow-hidden">
				<!-- Background network aesthetic glow -->
				<div class="absolute -right-16 -top-16 w-48 h-48 bg-cyan-500/10 rounded-full blur-3xl pointer-events-none"></div>

				<div class="flex items-center justify-between border-b border-zinc-800 pb-4">
					<div class="flex flex-col">
						<span class="text-[10px] font-mono text-zinc-500 uppercase tracking-wider">Active Solver</span>
						<h2 class="text-lg font-black text-zinc-100 mt-0.5">Membuss Session Manager</h2>
					</div>
					<span class="px-2.5 py-1 text-xs font-mono font-bold uppercase rounded border border-cyan-800/40 bg-cyan-950/40 text-cyan-400 animate-pulse">
						{statusBadgeText}
					</span>
				</div>

				<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
					<!-- Statistics panel -->
					<div class="flex flex-col gap-4 bg-zinc-950/40 border border-zinc-850 p-4 rounded-xl">
						<h3 class="font-bold text-xs text-zinc-400 font-mono uppercase tracking-wider">Session Stats</h3>
						
						<div class="flex flex-col gap-2 font-mono text-xs">
							<div class="flex justify-between py-1 border-b border-zinc-900/50">
								<span class="text-zinc-500">Status</span>
								<span class="text-zinc-300 font-bold">{statStatusText}</span>
							</div>
							<div class="flex justify-between py-1 border-b border-zinc-900/50">
								<span class="text-zinc-500">Active Providers</span>
								<span class="text-zinc-300 font-bold">{statPeers} peers</span>
							</div>
							<div class="flex justify-between py-1 border-b border-zinc-900/50">
								<span class="text-zinc-500">Resolved Blocks</span>
								<span class="text-zinc-200 font-bold">{statBlocksResolved} / {statBlocksTotal}</span>
							</div>
							<div class="flex justify-between py-1">
								<span class="text-zinc-500">Verification scan</span>
								<span class="text-cyan-400 font-bold">{statAvailability}</span>
							</div>
						</div>

						<!-- Progress Bar -->
						<div class="w-full h-2 rounded-full bg-zinc-950 border border-zinc-850 overflow-hidden mt-2">
							<div 
								class="h-full bg-gradient-to-r from-cyan-500 to-blue-500 transition-all duration-300"
								style={`width: ${statBlocksTotal > 0 ? (statBlocksResolved * 100 / statBlocksTotal) : 0}%`}
							></div>
						</div>
					</div>

					<!-- Active Providers Panel -->
					<div class="flex flex-col gap-4 bg-zinc-950/40 border border-zinc-850 p-4 rounded-xl">
						<h3 class="font-bold text-xs text-zinc-400 font-mono uppercase tracking-wider">Active DHT Providers</h3>
						<div class="max-h-40 overflow-y-auto divide-y divide-zinc-900/60 font-mono text-[10px] text-zinc-500 p-2 border border-zinc-900 rounded bg-zinc-950/40">
							{#if activeProviders && activeProviders.length > 0}
								{#each activeProviders as prov}
									<div class="py-1 flex items-center justify-between select-all hover:text-zinc-300 transition-colors">
										<span>{prov}</span>
									</div>
								{/each}
							{:else}
								<div class="py-6 text-center italic">Waiting for providers...</div>
							{/if}
						</div>
					</div>
				</div>

				<!-- Piece Map visualizer -->
				<div class="bg-zinc-950/30 border border-zinc-850 p-4 rounded-xl flex flex-col gap-3">
					<div class="flex justify-between items-center">
						<h3 class="font-bold text-xs text-zinc-400 font-mono uppercase tracking-wider">Session Piece Map</h3>
						<div class="flex gap-3 text-[9px] font-mono uppercase text-zinc-500">
							<div class="flex items-center gap-1"><span class="w-2 h-2 rounded bg-zinc-800"></span> Queued</div>
							<div class="flex items-center gap-1"><span class="w-2 h-2 rounded bg-yellow-500/30"></span> Scanning</div>
							<div class="flex items-center gap-1"><span class="w-2 h-2 rounded bg-cyan-500/20"></span> Checked</div>
							<div class="flex items-center gap-1"><span class="w-2 h-2 rounded bg-cyan-500 animate-pulse"></span> Downloaded</div>
						</div>
					</div>

					<div class="grid grid-cols-8 sm:grid-cols-12 md:grid-cols-20 gap-1.5 p-2 bg-zinc-950/60 border border-zinc-900 rounded-lg">
						{#each pieceGrid as cell}
							<div 
								class={`aspect-square rounded-[3px] transition-all duration-300 ${
									cell === 'finished' ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.3)]' :
									cell === 'downloaded' ? 'bg-cyan-500 shadow-[0_0_6px_rgba(6,182,212,0.3)]' :
									cell === 'checked' ? 'bg-cyan-950/60 border border-cyan-800/40' :
									cell === 'scanning' ? 'bg-yellow-500/50 animate-pulse' :
									'bg-zinc-850'
								}`}
							></div>
						{/each}
					</div>
				</div>
			</div>
		{:else if data.NotFound}
			<!-- NOT FOUND REGULAR UI (No providers found / Lookup failed) -->
			<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 flex flex-col gap-4 text-center items-center py-12">
				<span class="text-4xl">⚠️</span>
				<h2 class="text-lg font-black text-zinc-200 mt-2">Content Identifier Not Pinned Locally</h2>
				
				{#if data.ResolveStatus === 2}
					<p class="text-xs text-amber-500 font-mono">{data.ResolveMessage}</p>
					<p class="text-xs text-zinc-500 max-w-sm mt-1">Reloading or clicking retry will query the Kademlia DHT for new active provider records.</p>
				{:else if data.ResolveStatus === 3}
					<p class="text-xs text-zinc-400 font-mono">{data.ResolveMessage}</p>
					<p class="text-xs text-zinc-500 max-w-sm mt-1">If you know a peer has this file, ask them to seal it on their node first.</p>
				{:else if data.ResolveStatus === 4}
					<p class="text-xs text-red-400 font-mono">{data.ResolveMessage}</p>
				{:else}
					<p class="text-xs text-zinc-400">DHT search returned no active hosts holding this Content ID.</p>
				{/if}

				<!-- Providers List -->
				<div class="w-full max-w-md bg-zinc-950 border border-zinc-850 rounded-lg p-3 mt-4 text-left">
					<span class="block text-[9px] font-mono text-zinc-500 uppercase tracking-wide">Providers query logs</span>
					<pre class="font-mono text-[10px] text-zinc-450 mt-1 max-h-24 overflow-y-auto whitespace-pre-wrap select-all leading-tight">
						{#if data.Providers && data.Providers.length > 0}
							{#each data.Providers as p}
								{p}{'\n'}
							{/each}
						{:else}
							No provider addresses resolved.
						{/if}
					</pre>
				</div>

				<button onclick={loadMID} class="px-5 py-2.5 rounded-xl bg-cyan-500 hover:bg-cyan-600 text-zinc-950 font-bold text-xs transition-all duration-200 mt-4">
					Retry DHT Lookup
				</button>
			</div>
		{:else}
			<!-- FOUND LOCALLY REGULAR UI -->
			<!-- Tab Toggles -->
			<div class="flex border-b border-zinc-800">
				<button 
					onclick={() => activeTab = 'info'}
					class={`px-6 py-3 font-semibold text-sm border-b-2 -mb-[2px] transition-all ${
						activeTab === 'info' ? 'border-cyan-500 text-cyan-400' : 'border-transparent text-zinc-500 hover:text-zinc-350'
					}`}
				>
					Content Details
				</button>
				<button 
					onclick={() => activeTab = 'dag'}
					class={`px-6 py-3 font-semibold text-sm border-b-2 -mb-[2px] transition-all ${
						activeTab === 'dag' ? 'border-cyan-500 text-cyan-400' : 'border-transparent text-zinc-500 hover:text-zinc-350'
					}`}
				>
					Merkle DAG Tree
				</button>
			</div>

			<!-- Tab 1: Info & Stats -->
			{#if activeTab === 'info'}
				<div class="grid grid-cols-1 md:grid-cols-3 gap-6">
					
					<!-- Metadata Card -->
					<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 md:col-span-2 flex flex-col gap-4">
						<h3 class="font-bold text-sm text-zinc-400 font-mono uppercase tracking-wider border-b border-zinc-800 pb-2">
							Block Metadata
						</h3>

						<dl class="grid grid-cols-3 gap-y-3.5 text-xs">
							<dt class="text-zinc-500 font-mono">Payload Size</dt>
							<dd class="col-span-2 text-zinc-200 font-bold font-mono">
								{formatBytes(data.Size)} <span class="text-zinc-500 font-normal font-sans">({data.Size} bytes)</span>
							</dd>

							<dt class="text-zinc-500 font-mono">DAG Blocks</dt>
							<dd class="col-span-2 text-zinc-200 font-bold font-mono">{data.Blocks}</dd>

							<dt class="text-zinc-500 font-mono">Content Type</dt>
							<dd class="col-span-2 text-zinc-300 font-mono">{data.ContentType || 'application/octet-stream'}</dd>

							<dt class="text-zinc-500 font-mono">Pin Sealer Status</dt>
							<dd class="col-span-2 flex items-center gap-2">
								<span class={`font-bold font-mono uppercase ${data.Sealed ? 'text-emerald-400' : 'text-amber-500'}`}>
									{data.Sealed ? 'SEALED' : 'UNSEALED'}
								</span>
								<span class="text-[10px] text-zinc-500 font-sans">({data.Sealers} hosts holding, {data.AnchorSealers} anchors)</span>
							</dd>

							<dt class="text-zinc-500 font-mono">Merkle Codec</dt>
							<dd class="col-span-2 text-zinc-400 font-mono">0x{data.Codec.toString(16)}</dd>
						</dl>

						<!-- Actions -->
						<div class="flex items-center gap-2 mt-4 pt-4 border-t border-zinc-800/80">
							<!-- Direct Open View -->
							{#if data.MemFSType === 'dir'}
								<a href={`${base.replace('/explorer', '')}/mem/${midVal}/`} target="_blank" class="px-4 py-2 rounded-lg bg-cyan-500 hover:bg-cyan-600 text-zinc-950 font-bold text-xs transition-colors">
									Open Gateway Directory
								</a>
							{:else}
								<a href={`${base.replace('/explorer', '')}/mem/${midVal}`} target="_blank" class="px-4 py-2 rounded-lg bg-cyan-500 hover:bg-cyan-600 text-zinc-950 font-bold text-xs transition-colors">
									View Payload File
								</a>
								<a 
									href={`${base.replace('/explorer', '')}/mem/${midVal}?download=1&filename=${encodeURIComponent(data.Name || (midVal + '.bin'))}`} 
									class="px-4 py-2 rounded-lg bg-zinc-800 hover:bg-zinc-700 text-zinc-200 border border-zinc-750 font-bold text-xs transition-colors"
								>
									Download
								</a>
							{/if}

							<!-- Seal/Unseal Toggle -->
							{#if data.Sealed}
								<button onclick={() => runAction('unseal')} class="px-4 py-2 rounded-lg bg-red-950/40 hover:bg-red-950/60 text-red-400 border border-red-900/40 text-xs font-bold transition-colors ml-auto">
									Unseal (Unpin)
								</button>
							{:else}
								<button onclick={() => runAction('seal')} class="px-4 py-2 rounded-lg bg-emerald-950/60 hover:bg-emerald-950/80 text-emerald-400 border border-emerald-800/30 text-xs font-bold transition-colors ml-auto">
									Seal (Pin)
								</button>
							{/if}
						</div>
					</div>

					<!-- Erasure Coding Redundancy -->
					<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 flex flex-col justify-between">
						<div>
							<h3 class="font-bold text-sm text-zinc-400 font-mono uppercase tracking-wider border-b border-zinc-800 pb-2">
								Erasure Coding
							</h3>
							
							<dl class="grid grid-cols-2 gap-y-3.5 text-xs font-mono mt-4">
								<dt class="text-zinc-500">Shard Layout</dt>
								<dd class="text-zinc-200 font-bold text-right">{data.DataShards} data + {data.ParityShards} parity</dd>

								<dt class="text-zinc-500">Total Shards</dt>
								<dd class="text-zinc-200 font-bold text-right">{data.TotalShards} blocks</dd>

								<dt class="text-zinc-500">Node Failure Max</dt>
								<dd class="text-zinc-200 font-bold text-right">{data.ParityShards} hosts offline</dd>
							</dl>
						</div>

						<div class="mt-6 border-t border-zinc-800/80 pt-4 flex items-center justify-between text-xs">
							<span class="text-zinc-500 font-mono">Redundancy Status</span>
							<span class="inline-flex items-center gap-1 px-2.5 py-0.5 rounded text-[11px] font-bold bg-emerald-950/40 text-emerald-400 border border-emerald-800/30">
								{data.HealthLabel} ({data.Health})
							</span>
						</div>
					</div>

					<!-- DIRECTORY VIEWER (if type == dir) -->
					{#if data.MemFSType === 'dir'}
						<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 md:col-span-3 flex flex-col gap-4">
							<h3 class="font-bold text-sm text-zinc-300 tracking-tight">
								Directory Contents ({data.MemFSEntries ? data.MemFSEntries.length : 0} entries)
							</h3>
							
							{#if data.MemFSEntries && data.MemFSEntries.length > 0}
								<div class="overflow-x-auto">
									<table class="w-full text-left border-collapse text-sm">
										<thead>
											<tr class="border-b border-zinc-800/60 text-zinc-500 font-mono text-xs uppercase bg-zinc-950/20">
												<th class="py-2.5 px-4 font-semibold">Name</th>
												<th class="py-2.5 px-4 font-semibold">Type</th>
												<th class="py-2.5 px-4 font-semibold">Size</th>
												<th class="py-2.5 px-4 font-semibold text-right">Content Address</th>
											</tr>
										</thead>
										<tbody class="divide-y divide-zinc-850/30">
											{#each data.MemFSEntries as file}
												<tr class="hover:bg-zinc-850/20 transition-colors">
													<td class="py-3 px-4">
														<a 
															href={`${base}/mid/${file.mid}`} 
															class="font-bold text-zinc-200 hover:text-cyan-400 hover:underline flex items-center gap-2"
														>
															<span>{file.type === 'dir' ? '📁' : '📄'}</span>
															<span>{file.name}</span>
														</a>
													</td>
													<td class="py-3 px-4 font-mono text-xs text-zinc-500 uppercase">{file.type}</td>
													<td class="py-3 px-4 font-mono text-xs text-zinc-400">
														{file.type === 'dir' ? '—' : formatBytes(file.size)}
													</td>
													<td class="py-3 px-4 font-mono text-xs text-zinc-500 text-right">
														<a href={`${base}/mid/${file.mid}`} class="hover:text-cyan-400 hover:underline">
															{file.mid.slice(0, 16)}...
														</a>
													</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							{:else}
								<div class="py-8 text-center text-zinc-600 italic">Empty directory tree</div>
							{/if}
						</div>
					{/if}

					<!-- SYMLINK VIEWER -->
					{#if data.MemFSType === 'symlink'}
						<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 md:col-span-3 flex flex-col gap-2">
							<h3 class="font-bold text-sm text-zinc-400 font-mono uppercase tracking-wider">Symlink Redirect Target</h3>
							<code class="bg-zinc-950 border border-zinc-850 p-3 rounded-lg font-mono text-xs text-cyan-400 select-all break-all block mt-1">
								{data.SymlinkTarget}
							</code>
						</div>
					{/if}

					<!-- Providers (DHT) -->
					<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 md:col-span-3 flex flex-col gap-4">
						<h3 class="font-bold text-sm text-zinc-400 font-mono uppercase tracking-wider border-b border-zinc-800 pb-2">
							DHT Content Providers ({data.Providers ? data.Providers.length : 0} nodes reporting)
						</h3>
						{#if data.Providers && data.Providers.length > 0}
							<ul class="flex flex-col gap-2">
								{#each data.Providers as prov}
									<li class="bg-zinc-950/60 border border-zinc-850 px-4 py-2.5 rounded-lg font-mono text-xs text-zinc-350 break-all select-all flex items-center justify-between group hover:border-zinc-800 transition-colors">
										<span>{prov}</span>
									</li>
								{/each}
							</ul>
						{:else}
							<div class="py-4 text-center text-zinc-550 italic text-xs leading-none">
								No external peer providers registered in Kademlia for this Content ID.
							</div>
						{/if}
					</div>

				</div>
			{/if}

			<!-- Tab 2: DAG Visualizer -->
			{#if activeTab === 'dag'}
				<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 flex flex-col gap-4 shadow-lg">
					<div>
						<h2 class="text-lg font-black tracking-tight text-zinc-150">Merkle DAG Block Structure</h2>
						<p class="text-xs text-zinc-550 mt-1 leading-relaxed">
							Expand nodes recursively to view Merkle link bindings. Blocks are verified against multihash addresses lazily fetched from <code class="bg-zinc-950 px-1 py-0.5 rounded font-mono">/mem/&lt;mid&gt;?format=dag-json</code>.
						</p>
					</div>

					<div class="p-4 bg-zinc-950/40 border border-zinc-850 rounded-xl max-h-[500px] overflow-y-auto mt-2">
						<DagNode mid={midVal} depth={0} />
					</div>
				</div>
			{/if}

		{/if}
	{/if}
</div>
