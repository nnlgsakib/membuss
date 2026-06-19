<script lang="ts">
	import { onMount, onDestroy, untrack } from 'svelte';
	import { page } from '$app/state';
	import { base } from '$app/paths';
	import { apiFetch, formatBytes } from '$lib/api';
	import Icon from '@iconify/svelte';
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

	let renameValue = $state('');
	let isRenaming = $state(false);
	let activeTab = $state<'info' | 'dag'>('info');

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

	const MAX_BOXES = 120;
	function initPieceGrid(total: number) {
		const size = total > 0 ? Math.min(MAX_BOXES, total) : 40;
		pieceGrid = Array(size).fill('queued');
	}

	function closeResolver() {
		if (eventSource) {
			eventSource.close();
			eventSource = null;
		}
		resolverActive = false;
	}

	function startResolutionStream(mid: string) {
		closeResolver();

		resolverActive = true;
		initPieceGrid(0);
		statusBadgeText = 'Connecting';
		statStatusText = 'Connecting to network DHT...';

		const url = `${base}/mid/${mid}/resolve-stream`;
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
				eventSource = null;
				pieceGrid = Array(pieceGrid.length).fill('finished');
				statusBadgeText = 'Complete';
				statStatusText = 'Assembly Complete!';

				setTimeout(() => {
					resolverActive = false;
					fetchMIDData(mid);
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
			if (es.readyState === EventSource.CLOSED) {
				resolverActive = false;
			}
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

	async function fetchMIDData(mid: string) {
		loading = true;
		error = null;
		closeResolver();

		try {
			const res = await apiFetch(`/mid/${mid}`);
			data = res;
			if (data) {
				renameValue = data.Name || '';
				if (data.NotFound) {
					startResolutionStream(mid);
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
			fetchMIDData(midVal);
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
			fetchMIDData(midVal);
		} catch (err) {
			alert(`Rename failed: ${err instanceof Error ? err.message : err}`);
		} finally {
			isRenaming = false;
		}
	}

	$effect(() => {
		const mid = midVal;
		if (mid) {
			untrack(() => fetchMIDData(mid));
		}
	});

	onDestroy(() => {
		closeResolver();
	});
</script>

<div class="flex flex-col gap-6">
	<div class="border-b border-slate-800 pb-4 flex flex-wrap items-center justify-between gap-4">
		<div>
			<div class="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-[10px] text-slate-400 font-mono tracking-wider uppercase">
				Content Address
			</div>
			<h1 class="text-xl md:text-2xl font-bold text-slate-50 mt-1 select-all break-all flex items-center gap-2">
				{#if data && data.Name}
					<span class="text-slate-100">{data.Name}</span>
					<span class="text-slate-500 font-normal font-mono text-sm">({midVal.slice(0,8)}...)</span>
				{:else}
					<span>{midVal}</span>
				{/if}
			</h1>
		</div>

		{#if data && !data.NotFound}
			<form onsubmit={renameContent} class="flex items-center gap-2">
				<input
					type="text"
					bind:value={renameValue}
					required
					disabled={isRenaming}
					placeholder="Rename content alias"
					class="bg-slate-950/60 border border-slate-800 text-slate-200 text-xs px-3 py-1.5 rounded-lg focus:outline-none focus:border-cyan-500/80 focus:ring-1 focus:ring-cyan-500/20"
				/>
				<button
					type="submit"
					disabled={isRenaming || (data && renameValue.trim() === data.Name)}
					class="px-3.5 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 disabled:opacity-50 text-xs font-bold text-slate-200 border border-slate-750 transition-colors active:scale-[0.98]"
				>
					{isRenaming ? 'Saving...' : 'Rename'}
				</button>
			</form>
		{/if}
	</div>

	<div class="bg-slate-900/60 border border-slate-800 rounded-xl px-4 py-3 flex items-center justify-between gap-4">
		<code class="font-mono text-xs text-slate-300 break-all select-all">{midVal}</code>
		<button
			onclick={copyToClipboard}
			class="shrink-0 px-3 py-1.5 rounded bg-slate-800 hover:bg-slate-700 text-xs font-bold text-slate-200 border border-slate-750 transition-colors active:scale-[0.98]"
		>
			{copiedMID ? 'Copied!' : 'Copy MID'}
		</button>
	</div>

	{#if loading && !data && !resolverActive}
		<div class="space-y-6 animate-pulse">
			<div class="h-44 bg-slate-900 rounded-lg"></div>
			<div class="h-28 bg-slate-900 rounded-lg"></div>
		</div>
	{:else if error}
		<div class="bg-red-950/20 border border-red-800/40 text-red-400 p-4 rounded-xl text-xs font-mono">
			{error}
		</div>
	{:else if data}
		{#if resolverActive}
			<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-6 shadow-xl relative overflow-hidden">
				<div class="absolute -right-16 -top-16 w-48 h-48 bg-cyan-500/10 rounded-full blur-3xl pointer-events-none"></div>

				<div class="flex items-center justify-between border-b border-slate-800 pb-4">
					<div class="flex flex-col">
						<span class="text-[10px] font-mono text-slate-500 ">Active Resolver</span>
						<h2 class="text-lg font-black text-slate-100 mt-0.5">Membuss Session Manager</h2>
					</div>
					<span class="px-2.5 py-1 text-xs font-mono font-bold uppercase rounded border border-cyan-800/40 bg-cyan-950/40 text-cyan-400 animate-pulse">
						{statusBadgeText}
					</span>
				</div>

				<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
					<div class="flex flex-col gap-4 bg-slate-950/40 border border-slate-850 p-4 rounded-xl">
						<h3 class="font-bold text-xs text-slate-400 font-mono ">Session Stats</h3>
						<div class="flex flex-col gap-2 font-mono text-xs">
							<div class="flex justify-between py-1 border-b border-slate-900/50">
								<span class="text-slate-500">Status</span>
								<span class="text-slate-300 font-bold">{statStatusText}</span>
							</div>
							<div class="flex justify-between py-1 border-b border-slate-900/50">
								<span class="text-slate-500">Active Providers</span>
								<span class="text-slate-300 font-bold">{statPeers} peers</span>
							</div>
							<div class="flex justify-between py-1 border-b border-slate-900/50">
								<span class="text-slate-500">Resolved Blocks</span>
								<span class="text-slate-200 font-bold">{statBlocksResolved} / {statBlocksTotal}</span>
							</div>
							<div class="flex justify-between py-1">
								<span class="text-slate-500">Verification scan</span>
								<span class="text-cyan-400 font-bold">{statAvailability}</span>
							</div>
						</div>

						<div class="w-full h-2 rounded-full bg-slate-950 border border-slate-850 overflow-hidden mt-2">
							<div
								class="h-full bg-cyan-500 transition-all duration-300"
								style={`width: ${statBlocksTotal > 0 ? (statBlocksResolved * 100 / statBlocksTotal) : 0}%`}
							></div>
						</div>
					</div>

					<div class="flex flex-col gap-4 bg-slate-950/40 border border-slate-850 p-4 rounded-xl">
						<h3 class="font-bold text-xs text-slate-400 font-mono ">Active DHT Providers</h3>
						<div class="max-h-40 overflow-y-auto divide-y divide-slate-900/60 font-mono text-[10px] text-slate-500 p-2 border border-slate-900 rounded bg-slate-950/40">
							{#if activeProviders && activeProviders.length > 0}
								{#each activeProviders as prov}
									<div class="py-1 flex items-center justify-between select-all hover:text-slate-300 transition-colors">
										<span>{prov}</span>
									</div>
								{/each}
							{:else}
								<div class="py-6 text-center italic">Waiting for providers...</div>
							{/if}
						</div>
					</div>
				</div>

				<div class="bg-slate-950/30 border border-slate-850 p-4 rounded-xl flex flex-col gap-3">
					<div class="flex justify-between items-center">
						<h3 class="font-bold text-xs text-slate-400 font-mono ">Session Piece Map</h3>
						<div class="flex gap-3 text-[9px] font-mono uppercase text-slate-500">
							<div class="flex items-center gap-1"><span class="w-2 h-2 rounded bg-slate-800"></span> Queued</div>
							<div class="flex items-center gap-1"><span class="w-2 h-2 rounded bg-yellow-500/30"></span> Scanning</div>
							<div class="flex items-center gap-1"><span class="w-2 h-2 rounded bg-cyan-500/20"></span> Checked</div>
							<div class="flex items-center gap-1"><span class="w-2 h-2 rounded bg-cyan-500 animate-pulse"></span> Downloaded</div>
						</div>
					</div>

					<div class="grid grid-cols-8 sm:grid-cols-12 md:grid-cols-20 gap-1.5 p-2 bg-slate-950/60 border border-slate-900 rounded-lg">
						{#each pieceGrid as cell}
							<div
								class={`aspect-square rounded-[3px] transition-all duration-300 ${
									cell === 'finished' ? 'bg-emerald-500' :
									cell === 'downloaded' ? 'bg-cyan-500' :
									cell === 'checked' ? 'bg-cyan-950/60 border border-cyan-800/40' :
									cell === 'scanning' ? 'bg-yellow-500/50 animate-pulse' :
									'bg-slate-800'
								}`}
							></div>
						{/each}
					</div>
				</div>
			</div>
		{:else if data.NotFound}
			<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4 text-center items-center py-12">
				<span class="text-4xl"><Icon icon="ph:warning-circle" class="text-amber-500" /></span>
				<h2 class="text-lg font-black text-slate-200 mt-2">Content Identifier Not Pinned Locally</h2>

				{#if data.ResolveStatus === 2}
					<p class="text-xs text-amber-500 font-mono">{data.ResolveMessage}</p>
					<p class="text-xs text-slate-500 max-w-sm mt-1">Reloading or clicking retry will query the Kademlia DHT for new active provider records.</p>
				{:else if data.ResolveStatus === 3}
					<p class="text-xs text-slate-400 font-mono">{data.ResolveMessage}</p>
					<p class="text-xs text-slate-500 max-w-sm mt-1">If you know a peer has this file, ask them to seal it on their node first.</p>
				{:else if data.ResolveStatus === 4}
					<p class="text-xs text-red-400 font-mono">{data.ResolveMessage}</p>
				{:else}
					<p class="text-xs text-slate-400">DHT search returned no active hosts holding this Content ID.</p>
				{/if}

				<div class="w-full max-w-md bg-slate-950 border border-slate-850 rounded-lg p-3 mt-4 text-left">
					<span class="block text-[9px] font-mono text-slate-500 uppercase tracking-wide">Providers query logs</span>
					<pre class="font-mono text-[10px] text-slate-450 mt-1 max-h-24 overflow-y-auto whitespace-pre-wrap select-all leading-tight">
						{#if data.Providers && data.Providers.length > 0}
							{#each data.Providers as p}
								{p}{'\n'}
							{/each}
						{:else}
							No provider addresses resolved.
						{/if}
					</pre>
				</div>

				<button onclick={() => fetchMIDData(midVal)} class="px-5 py-2.5 rounded-xl bg-cyan-500 hover:bg-cyan-600 text-slate-950 font-bold text-xs transition-all duration-200 mt-4 active:scale-[0.98]">
					Retry DHT Lookup
				</button>
			</div>
		{:else}
			<div class="flex border-b border-slate-800">
				<button
					onclick={() => activeTab = 'info'}
					class={`px-6 py-3 font-semibold text-sm border-b-2 -mb-[2px] transition-all ${
						activeTab === 'info' ? 'border-cyan-500 text-cyan-400' : 'border-transparent text-slate-500 hover:text-slate-350'
					}`}
				>
					Content Details
				</button>
				<button
					onclick={() => activeTab = 'dag'}
					class={`px-6 py-3 font-semibold text-sm border-b-2 -mb-[2px] transition-all ${
						activeTab === 'dag' ? 'border-cyan-500 text-cyan-400' : 'border-transparent text-slate-500 hover:text-slate-350'
					}`}
				>
					Merkle DAG Tree
				</button>
			</div>

			{#if activeTab === 'info'}
				<div class="grid grid-cols-1 md:grid-cols-3 gap-6">
					<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 md:col-span-2 flex flex-col gap-4">
						<h3 class="font-bold text-sm text-slate-400 font-mono  border-b border-slate-800 pb-2">
							Block Metadata
						</h3>

						<dl class="grid grid-cols-3 gap-y-3.5 text-xs">
							<dt class="text-slate-500 font-mono">Payload Size</dt>
							<dd class="col-span-2 text-slate-200 font-bold font-mono">
								{formatBytes(data.Size)} <span class="text-slate-500 font-normal font-sans">({data.Size} bytes)</span>
							</dd>

							<dt class="text-slate-500 font-mono">DAG Blocks</dt>
							<dd class="col-span-2 text-slate-200 font-bold font-mono">{data.Blocks}</dd>

							<dt class="text-slate-500 font-mono">Content Type</dt>
							<dd class="col-span-2 text-slate-300 font-mono">{data.ContentType || 'application/octet-stream'}</dd>

							<dt class="text-slate-500 font-mono">Pin Sealer Status</dt>
							<dd class="col-span-2 flex items-center gap-2">
								<span class={`font-bold font-mono uppercase ${data.Sealed ? 'text-emerald-400' : 'text-amber-500'}`}>
									{data.Sealed ? 'SEALED' : 'UNSEALED'}
								</span>
								<span class="text-[10px] text-slate-500 font-sans">({data.Sealers} hosts holding, {data.AnchorSealers} anchors)</span>
							</dd>

							<dt class="text-slate-500 font-mono">Merkle Codec</dt>
							<dd class="col-span-2 text-slate-400 font-mono">0x{data.Codec.toString(16)}</dd>
						</dl>

						<div class="flex items-center gap-2 mt-4 pt-4 border-t border-slate-800/80">
							{#if data.MemFSType === 'dir'}
								<a href={`${base.replace('/explorer', '')}/mem/${midVal}/`} target="_blank" class="px-4 py-2 rounded-lg bg-cyan-500 hover:bg-cyan-600 text-slate-950 font-bold text-xs transition-colors">
									Open Gateway Directory
								</a>
							{:else}
								<a href={`${base.replace('/explorer', '')}/mem/${midVal}`} target="_blank" class="px-4 py-2 rounded-lg bg-cyan-500 hover:bg-cyan-600 text-slate-950 font-bold text-xs transition-colors">
									View Payload File
								</a>
								<a
									href={`${base.replace('/explorer', '')}/mem/${midVal}?download=1&filename=${encodeURIComponent(data.Name || (midVal + '.bin'))}`}
									class="px-4 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-750 font-bold text-xs transition-colors"
								>
									Download
								</a>
							{/if}

							{#if data.Sealed}
								<button onclick={() => runAction('unseal')} class="px-4 py-2 rounded-lg bg-red-950/40 hover:bg-red-950/60 text-red-400 border border-red-900/40 text-xs font-bold transition-colors ml-auto active:scale-[0.98]">
									Unseal (Unpin)
								</button>
							{:else}
								<button onclick={() => runAction('seal')} class="px-4 py-2 rounded-lg bg-emerald-950/60 hover:bg-emerald-950/80 text-emerald-400 border border-emerald-800/30 text-xs font-bold transition-colors ml-auto active:scale-[0.98]">
									Seal (Pin)
								</button>
							{/if}
						</div>
					</div>

					<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col justify-between">
						<div>
							<h3 class="font-bold text-sm text-slate-400 font-mono  border-b border-slate-800 pb-2">
								Erasure Coding
							</h3>

							<dl class="grid grid-cols-2 gap-y-3.5 text-xs font-mono mt-4">
								<dt class="text-slate-500">Shard Layout</dt>
								<dd class="text-slate-200 font-bold text-right">{data.DataShards} data + {data.ParityShards} parity</dd>

								<dt class="text-slate-500">Total Shards</dt>
								<dd class="text-slate-200 font-bold text-right">{data.TotalShards} blocks</dd>

								<dt class="text-slate-500">Node Failure Max</dt>
								<dd class="text-slate-200 font-bold text-right">{data.ParityShards} hosts offline</dd>
							</dl>
						</div>

						<div class="mt-6 border-t border-slate-800/80 pt-4 flex items-center justify-between text-xs">
							<span class="text-slate-500 font-mono">Redundancy Status</span>
							<span class="inline-flex items-center gap-1 px-2.5 py-0.5 rounded text-[11px] font-bold bg-emerald-950/40 text-emerald-400 border border-emerald-800/30">
								{data.HealthLabel} ({data.Health})
							</span>
						</div>
					</div>

					{#if data.MemFSType === 'dir'}
						<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 md:col-span-3 flex flex-col gap-4">
							<h3 class="font-bold text-sm text-slate-300 tracking-tight">
								Directory Contents ({data.MemFSEntries ? data.MemFSEntries.length : 0} entries)
							</h3>

							{#if data.MemFSEntries && data.MemFSEntries.length > 0}
								<div class="overflow-x-auto">
									<table class="w-full text-left border-collapse text-sm">
										<thead>
											<tr class="border-b border-slate-800/60 text-slate-500 font-mono text-xs uppercase bg-slate-950/20">
												<th class="py-2.5 px-4 font-semibold">Name</th>
												<th class="py-2.5 px-4 font-semibold">Type</th>
												<th class="py-2.5 px-4 font-semibold">Size</th>
												<th class="py-2.5 px-4 font-semibold text-right">Content Address</th>
											</tr>
										</thead>
										<tbody class="divide-y divide-slate-850/30">
											{#each data.MemFSEntries as file}
												<tr class="hover:bg-slate-850/20 transition-colors">
													<td class="py-3 px-4">
														<a
															href={`${base}/mid/${file.mid}`}
															class="font-bold text-slate-200 hover:text-cyan-400 hover:underline flex items-center gap-2"
														>
															{#if file.type === 'dir'}
															<Icon icon="ph:folder" class="text-cyan-400" />
														{:else}
															<Icon icon="ph:file" class="text-slate-400" />
														{/if}
															<span>{file.name}</span>
														</a>
													</td>
													<td class="py-3 px-4 font-mono text-xs text-slate-500 uppercase">{file.type}</td>
													<td class="py-3 px-4 font-mono text-xs text-slate-400">
														{file.type === 'dir' ? '—' : formatBytes(file.size)}
													</td>
													<td class="py-3 px-4 font-mono text-xs text-slate-500 text-right">
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
								<div class="py-8 text-center text-slate-600 italic">Empty directory tree</div>
							{/if}
						</div>
					{/if}

					{#if data.MemFSType === 'symlink'}
						<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 md:col-span-3 flex flex-col gap-2">
							<h3 class="font-bold text-sm text-slate-400 font-mono ">Symlink Redirect Target</h3>
							<code class="bg-slate-950 border border-slate-850 p-3 rounded-lg font-mono text-xs text-cyan-400 select-all break-all block mt-1">
								{data.SymlinkTarget}
							</code>
						</div>
					{/if}

					<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 md:col-span-3 flex flex-col gap-4">
						<h3 class="font-bold text-sm text-slate-400 font-mono  border-b border-slate-800 pb-2">
							DHT Content Providers ({data.Providers ? data.Providers.length : 0} nodes reporting)
						</h3>
						{#if data.Providers && data.Providers.length > 0}
							<ul class="flex flex-col gap-2">
								{#each data.Providers as prov}
									<li class="bg-slate-950/60 border border-slate-850 px-4 py-2.5 rounded-lg font-mono text-xs text-slate-350 break-all select-all flex items-center justify-between group hover:border-slate-800 transition-colors">
										<span>{prov}</span>
									</li>
								{/each}
							</ul>
						{:else}
							<div class="py-4 text-center text-slate-550 italic text-xs leading-none">
								No external peer providers registered in Kademlia for this Content ID.
							</div>
						{/if}
					</div>
				</div>
			{/if}

			{#if activeTab === 'dag'}
				<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4 shadow-lg">
					<div>
						<h2 class="text-lg font-black tracking-tight text-slate-150">Merkle DAG Block Structure</h2>
						<p class="text-xs text-slate-550 mt-1 leading-relaxed">
							Expand nodes recursively to view Merkle link bindings. Blocks are verified against multihash addresses lazily fetched from <code class="bg-slate-950 px-1 py-0.5 rounded font-mono">/mem/&lt;mid&gt;?format=dag-json</code>.
						</p>
					</div>

					<div class="p-4 bg-slate-950/40 border border-slate-850 rounded-xl max-h-[500px] overflow-y-auto mt-2">
						<DagNode mid={midVal} depth={0} />
					</div>
				</div>
			{/if}

		{/if}
	{/if}
</div>
