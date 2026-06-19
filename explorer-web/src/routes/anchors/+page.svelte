<script lang="ts">
	import { onMount } from 'svelte';
	import { apiFetch } from '$lib/api';

	interface AnchorInfo {
		PeerID: string;
		UptimeSecs: number;
		BlocksHeld: number;
		Anchors: number;
		Backlog: number;
		Synced: number;
	}

	interface AnchorRow {
		PeerID: string;
		Addrs: string[];
	}

	interface AnchorsData {
		Title: string;
		AnchorInfo: AnchorInfo;
		Anchors: AnchorRow[];
		AnchorMode: boolean;
	}

	let data = $state<AnchorsData | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let copiedId = $state<string | null>(null);

	async function loadAnchors() {
		try {
			const res = await apiFetch('/anchors');
			data = res;
			loading = false;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load anchor stats';
			loading = false;
		}
	}

	function copyToClipboard(text: string, id: string) {
		navigator.clipboard.writeText(text).then(() => {
			copiedId = id;
			setTimeout(() => {
				if (copiedId === id) copiedId = null;
			}, 1500);
		});
	}

	onMount(() => {
		loadAnchors();
		const interval = setInterval(loadAnchors, 10000);
		return () => clearInterval(interval);
	});
</script>

<div class="flex flex-col gap-6">
	<!-- Page Header -->
	<div class="border-b border-zinc-800 pb-4">
		<h1 class="text-2xl font-black text-zinc-50">Anchor Network Index</h1>
		<p class="text-xs text-zinc-500 mt-1">Full-sync persistence engines serving block redundancy across the network</p>
	</div>

	{#if loading && !data}
		<div class="space-y-6 animate-pulse">
			<div class="h-40 bg-zinc-900 rounded-lg w-full"></div>
			<div class="h-32 bg-zinc-900 rounded-lg w-full"></div>
		</div>
	{:else if error}
		<div class="bg-red-950/20 border border-red-800/40 text-red-400 p-4 rounded-xl text-xs font-mono">
			{error}
		</div>
	{:else if data}
		<!-- Local Anchor Engine Panel -->
		{#if data.AnchorMode}
			<div class="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden shadow-lg">
				<div class="px-6 py-4 bg-emerald-950/20 border-b border-zinc-850 flex items-center justify-between">
					<div class="flex items-center gap-2">
						<span class="w-2.5 h-2.5 rounded-full bg-emerald-500 shadow-[0_0_10px_#10b981]"></span>
						<h3 class="font-bold text-sm text-zinc-100">Local Sync Engine Status</h3>
					</div>
					<span class="text-[10px] font-mono font-bold tracking-wider px-2 py-0.5 rounded bg-emerald-900/40 text-emerald-400 border border-emerald-800/30 uppercase">
						active host
					</span>
				</div>

				<div class="p-6 grid grid-cols-2 md:grid-cols-4 gap-6 text-sm">
					<div>
						<span class="block text-[10px] font-mono text-zinc-500 uppercase">Engine ID</span>
						<button 
							onclick={() => copyToClipboard(data!.AnchorInfo.PeerID, 'local')}
							class="text-xs text-cyan-400 hover:text-cyan-300 font-mono mt-1 select-all hover:underline"
						>
							{data.AnchorInfo.PeerID.slice(0, 10)}...{data.AnchorInfo.PeerID.slice(-10)}
							<span>{copiedId === 'local' ? '✓' : ''}</span>
						</button>
					</div>
					<div>
						<span class="block text-[10px] font-mono text-zinc-500 uppercase">Blocks Held</span>
						<span class="block text-zinc-200 font-bold text-lg mt-1 font-mono">{data.AnchorInfo.BlocksHeld}</span>
					</div>
					<div>
						<span class="block text-[10px] font-mono text-zinc-500 uppercase">Backlog Queue</span>
						<span class="block text-zinc-200 font-bold text-lg mt-1 font-mono">{data.AnchorInfo.Backlog} blocks</span>
					</div>
					<div>
						<span class="block text-[10px] font-mono text-zinc-500 uppercase">Sync Progress</span>
						<span class="block text-zinc-200 font-bold text-lg mt-1 font-mono">{data.AnchorInfo.Synced} blocks</span>
					</div>
				</div>
			</div>
		{:else}
			<div class="bg-zinc-900/40 border border-zinc-800 rounded-xl p-6 flex items-center justify-between gap-4">
				<div class="flex items-center gap-3">
					<div class="w-8 h-8 rounded-full bg-zinc-800/80 border border-zinc-700 flex items-center justify-center text-sm">
						🔒
					</div>
					<div>
						<h4 class="text-sm font-bold text-zinc-300">Local Anchor Daemon Offline</h4>
						<p class="text-xs text-zinc-500 mt-0.5">This node is running in peer mode and does not fully index foreign Content IDs.</p>
					</div>
				</div>
				<span class="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-zinc-850 text-zinc-500 border border-zinc-800">
					PASSIVE
				</span>
			</div>
		{/if}

		<!-- Registered Network Anchors Registry -->
		<div class="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
			<div class="px-6 py-4 bg-zinc-950/40 border-b border-zinc-800 flex items-center justify-between">
				<h3 class="font-bold text-sm text-zinc-300">Registered Redundancy Servers</h3>
				<span class="px-2.5 py-1 rounded bg-zinc-800 border border-zinc-750 text-xs font-mono text-cyan-400">
					{data.Anchors ? data.Anchors.length : 0} nodes
				</span>
			</div>

			{#if data.Anchors && data.Anchors.length > 0}
				<div class="overflow-x-auto">
					<table class="w-full text-left border-collapse text-sm">
						<thead>
							<tr class="border-b border-zinc-800/60 text-zinc-500 font-mono text-xs uppercase bg-zinc-950/20">
								<th class="py-3 px-6 font-semibold w-1/3">Anchor Peer ID</th>
								<th class="py-3 px-6 font-semibold">Discovery Addresses</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-zinc-850/40">
							{#each data.Anchors as row}
								<tr class="hover:bg-zinc-850/25 transition-colors group">
									<!-- Peer ID -->
									<td class="py-4 px-6 font-mono text-xs">
										<div class="flex items-center gap-2">
											<span class="text-zinc-200">{row.PeerID}</span>
											<button 
												onclick={() => copyToClipboard(row.PeerID, row.PeerID)}
												class="text-[10px] text-zinc-600 hover:text-zinc-300 opacity-0 group-hover:opacity-100 transition-opacity"
												title="Copy ID"
											>
												{copiedId === row.PeerID ? 'Copied' : 'Copy'}
											</button>
										</div>
									</td>

									<!-- Addrs -->
									<td class="py-4 px-6">
										{#if row.Addrs && row.Addrs.length > 0}
											<div class="flex flex-col gap-1">
												{#each row.Addrs as addr}
													<span class="font-mono text-[11px] text-zinc-450 hover:text-zinc-300 select-all break-all">
														{addr}
													</span>
												{/each}
											</div>
										{:else}
											<span class="text-xs text-zinc-600 italic">No static dials listed</span>
										{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else}
				<div class="py-16 text-center flex flex-col items-center justify-center gap-3">
					<span class="text-3xl text-zinc-700">📌</span>
					<div class="text-sm font-semibold text-zinc-450">No Remote Anchors Discovered</div>
					<p class="text-xs text-zinc-550 max-w-xs leading-relaxed">
						There are currently no external backup/sync servers known. Sealed content resides entirely on local hosts.
					</p>
				</div>
			{/if}
		</div>
	{/if}
</div>
