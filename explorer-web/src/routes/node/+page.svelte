<script lang="ts">
	import { onMount } from 'svelte';
	import { apiFetch, formatBytes } from '$lib/api';
	import { base } from '$app/paths';

	interface NodeInfo {
		PeerID: string;
		Addrs: string[];
		Version: string;
		Build: string;
		AnchorMode: boolean;
	}

	interface KeyringKey {
		Name: string;
		MemNSName: string;
		Type: string;
		CreatedAt: string;
	}

	interface NodeData {
		Title: string;
		NodeInfo: NodeInfo;
		StoreBytes: number;
		SealedCount: number;
		Keys: KeyringKey[];
	}

	let data = $state<NodeData | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let copiedId = $state<string | null>(null);

	async function loadNode() {
		try {
			const res = await apiFetch('/node');
			data = res;
			loading = false;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to load node details';
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

	function formatDate(dateStr: string): string {
		try {
			const d = new Date(dateStr);
			return d.toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
		} catch {
			return dateStr;
		}
	}

	onMount(() => {
		loadNode();
	});
</script>

<div class="flex flex-col gap-6">
	<!-- Page Header -->
	<div class="border-b border-zinc-800 pb-4">
		<h1 class="text-2xl font-black text-zinc-50">Local Daemon Node Parameters</h1>
		<p class="text-xs text-zinc-500 mt-1">Host node keys, listener network bindings, and publisher keyring records</p>
	</div>

	{#if loading && !data}
		<div class="grid grid-cols-1 md:grid-cols-2 gap-6 animate-pulse">
			<div class="h-44 bg-zinc-900 rounded-lg"></div>
			<div class="h-44 bg-zinc-900 rounded-lg"></div>
			<div class="h-40 bg-zinc-900 rounded-lg md:col-span-2"></div>
		</div>
	{:else if error}
		<div class="bg-red-950/20 border border-red-800/40 text-red-400 p-4 rounded-xl text-xs font-mono">
			{error}
		</div>
	{:else if data}
		<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
			<!-- Identity Card -->
			<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 flex flex-col gap-4">
				<h3 class="font-bold text-sm text-zinc-400 font-mono uppercase tracking-wider border-b border-zinc-800 pb-2">
					Identity & Credentials
				</h3>
				<dl class="grid grid-cols-3 gap-y-3 text-xs leading-relaxed">
					<dt class="text-zinc-500 font-mono">Peer ID</dt>
					<dd class="col-span-2 font-mono text-zinc-300 break-all select-all flex items-center gap-1">
						{data.NodeInfo.PeerID}
						<button 
							onclick={() => copyToClipboard(data!.NodeInfo.PeerID, 'peerid')}
							class="text-[10px] text-cyan-500 hover:text-cyan-300 hover:underline"
						>
							{copiedId === 'peerid' ? '[Copied]' : '[Copy]'}
						</button>
					</dd>

					<dt class="text-zinc-500 font-mono">Daemon Version</dt>
					<dd class="col-span-2 text-zinc-300 font-mono">{data.NodeInfo.Version}</dd>

					<dt class="text-zinc-500 font-mono">Build Target</dt>
					<dd class="col-span-2 text-zinc-300 font-mono uppercase">{data.NodeInfo.Build}</dd>

					<dt class="text-zinc-500 font-mono">Anchor Engine</dt>
					<dd class="col-span-2">
						<span class={`font-bold ${data.NodeInfo.AnchorMode ? 'text-emerald-400' : 'text-zinc-500'}`}>
							{data.NodeInfo.AnchorMode ? 'ACTIVE' : 'INACTIVE'}
						</span>
					</dd>
				</dl>
			</div>

			<!-- Storage Metrics -->
			<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 flex flex-col gap-4">
				<h3 class="font-bold text-sm text-zinc-400 font-mono uppercase tracking-wider border-b border-zinc-800 pb-2">
					Storage Footprint
				</h3>
				<dl class="grid grid-cols-3 gap-y-3 text-xs leading-relaxed">
					<dt class="text-zinc-500 font-mono">Database size</dt>
					<dd class="col-span-2 text-zinc-200 font-bold font-mono">
						{formatBytes(data.StoreBytes)} <span class="text-zinc-500 text-[10px] font-normal font-sans">({data.StoreBytes} bytes)</span>
					</dd>

					<dt class="text-zinc-500 font-mono">Pinned Roots</dt>
					<dd class="col-span-2 text-zinc-200 font-mono">{data.SealedCount} sealed Content IDs</dd>
				</dl>
			</div>

			<!-- Listen Interfaces -->
			<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 md:col-span-2 flex flex-col gap-4">
				<h3 class="font-bold text-sm text-zinc-400 font-mono uppercase tracking-wider border-b border-zinc-800 pb-2">
					Listen Interfaces & Multiaddresses
				</h3>
				{#if data.NodeInfo.Addrs && data.NodeInfo.Addrs.length > 0}
					<ul class="flex flex-col gap-2">
						{#each data.NodeInfo.Addrs as addr}
							<li class="bg-zinc-950/60 border border-zinc-850 px-4 py-2.5 rounded-lg font-mono text-xs text-zinc-300 flex items-center justify-between group hover:border-zinc-800 transition-colors">
								<span class="select-all break-all">{addr}</span>
								<button 
									onclick={() => copyToClipboard(addr, addr)}
									class="text-[10px] text-cyan-500 hover:text-cyan-300 hover:underline opacity-0 group-hover:opacity-100 transition-opacity"
								>
									{copiedId === addr ? 'Copied ✓' : 'Copy'}
								</button>
							</li>
						{/each}
					</ul>
				{:else}
					<div class="text-zinc-500 italic text-xs py-4 text-center">No active listeners configured. Node is outbound-only.</div>
				{/if}
			</div>

			<!-- Cryptographic KeyRing -->
			<div class="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden md:col-span-2">
				<div class="px-6 py-4 bg-zinc-950/40 border-b border-zinc-800 flex items-center justify-between">
					<h3 class="font-bold text-sm text-zinc-300">Cryptographic KeyRing</h3>
					<span class="px-2.5 py-0.5 rounded bg-zinc-800 text-xs font-mono text-zinc-400">
						{data.Keys ? data.Keys.length : 0} identity key{data.Keys && data.Keys.length === 1 ? '' : 's'}
					</span>
				</div>

				{#if data.Keys && data.Keys.length > 0}
					<div class="overflow-x-auto">
						<table class="w-full text-left border-collapse text-sm">
							<thead>
								<tr class="border-b border-zinc-800/60 text-zinc-500 font-mono text-xs uppercase bg-zinc-950/20">
									<th class="py-3 px-6 font-semibold">Key Name</th>
									<th class="py-3 px-6 font-semibold">MemNS Domain</th>
									<th class="py-3 px-6 font-semibold w-24">Key Type</th>
									<th class="py-3 px-6 font-semibold w-48 text-right">Created At</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-zinc-850/40">
								{#each data.Keys as key}
									<tr class="hover:bg-zinc-850/25 transition-colors">
										<!-- Key Name -->
										<td class="py-3.5 px-6 font-bold text-zinc-200 font-mono text-xs">{key.Name}</td>
										
										<!-- MemNS Domain -->
										<td class="py-3.5 px-6 font-mono text-xs">
											{#if key.MemNSName}
												<a 
													href={`${base}/memns/${key.MemNSName.replace('/memns/', '')}`} 
													class="text-cyan-400 hover:underline hover:text-cyan-300"
												>
													{key.MemNSName}
												</a>
											{:else}
												<span class="text-zinc-600 italic">No bound domain</span>
											{/if}
										</td>

										<!-- Type -->
										<td class="py-3.5 px-6 text-zinc-400 font-mono text-xs">{key.Type}</td>

										<!-- Created At -->
										<td class="py-3.5 px-6 text-zinc-500 text-right font-mono text-xs">
											{formatDate(key.CreatedAt)}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{:else}
					<div class="py-12 text-center text-zinc-550 flex flex-col items-center justify-center gap-2">
						<span>🔑</span>
						<div class="text-xs font-semibold text-zinc-400">Keyring is Empty</div>
						<p class="text-[11px] text-zinc-600 max-w-xs">Generate a local key pair using `membuss-cli keyring generate` to anchor name records.</p>
					</div>
				{/if}
			</div>
		</div>
	{/if}
</div>
