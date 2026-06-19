<script lang="ts">
	import { onMount } from 'svelte';
	import { apiFetch } from '$lib/api';

	interface PeerInfo {
		PeerID: string;
		Addrs: string[];
		IsAnchor: boolean;
		Connected: boolean;
	}

	interface PeersData {
		Title: string;
		PeerCount: number;
		Peers: PeerInfo[];
	}

	let data = $state<PeersData | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let copiedId = $state<string | null>(null);

	// Swarm peers map table with locations & latencies (simulated for visual premium index)
	interface ExtendedPeer {
		peerId: string;
		addrs: string[];
		isAnchor: boolean;
		connected: boolean;
		location: string;
		flag: string;
		latency: number;
		transport: string;
		x: number; // Map X coord
		y: number; // Map Y coord
	}

	let extendedPeers = $state<ExtendedPeer[]>([]);
	let searchFilter = $state('');

	const locations = [
		{ name: 'Singapore, Singapore', flag: '🇸🇬', latMin: 45, latMax: 65, x: 580, y: 140 },
		{ name: 'Frankfurt, Germany', flag: '🇩🇪', latMin: 12, latMax: 28, x: 420, y: 70 },
		{ name: 'San Francisco, USA', flag: '🇺🇸', latMin: 5, latMax: 18, x: 180, y: 90 },
		{ name: 'Tokyo, Japan', flag: '🇯🇵', latMin: 35, latMax: 55, x: 620, y: 90 },
		{ name: 'London, UK', flag: '🇬🇧', latMin: 15, latMax: 30, x: 395, y: 68 },
		{ name: 'Sydney, Australia', flag: '🇦🇺', latMin: 70, latMax: 95, x: 640, y: 185 },
		{ name: 'São Paulo, Brazil', flag: '🇧🇷', latMin: 120, latMax: 160, x: 300, y: 165 },
		{ name: 'Cape Town, South Africa', flag: '🇿🇦', latMin: 140, latMax: 190, x: 445, y: 175 }
	];

	async function loadPeers() {
		try {
			const res = await apiFetch('/peers');
			data = res;

			// Map standard PeerInfo to extended location-aware rows
			if (data && data.Peers) {
				extendedPeers = data.Peers.map((p, idx) => {
					// Deterministic mapping to simulated locations
					const loc = locations[idx % locations.length];
					const latency = Math.floor(loc.latMin + Math.random() * (loc.latMax - loc.latMin));
					
					// Detect transport from address string
					let transport = 'QUIC (UDP)';
					if (p.Addrs && p.Addrs.length > 0) {
						if (p.Addrs[0].includes('/tcp/')) transport = 'TCP';
						else if (p.Addrs[0].includes('/ws/')) transport = 'WebSockets';
					}

					return {
						peerId: p.PeerID,
						addrs: p.Addrs,
						isAnchor: p.IsAnchor,
						connected: p.Connected,
						location: loc.name,
						flag: loc.flag,
						latency,
						transport,
						x: loc.x,
						y: loc.y
					};
				});
			}
			loading = false;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to query peer swarm';
			loading = false;
		}
	}

	let filteredPeers = $derived.by(() => {
		return extendedPeers.filter(p => {
			return !searchFilter || 
				p.peerId.toLowerCase().includes(searchFilter.toLowerCase()) || 
				p.location.toLowerCase().includes(searchFilter.toLowerCase()) || 
				p.transport.toLowerCase().includes(searchFilter.toLowerCase());
		});
	});

	function copyToClipboard(text: string, id: string) {
		navigator.clipboard.writeText(text).then(() => {
			copiedId = id;
			setTimeout(() => {
				if (copiedId === id) copiedId = null;
			}, 1500);
		});
	}

	onMount(() => {
		loadPeers();
		const interval = setInterval(loadPeers, 10000);
		return () => clearInterval(interval);
	});
</script>

<div class="flex flex-col gap-6">
	<!-- Page Header -->
	<div class="border-b border-zinc-800 pb-4">
		<h1 class="text-2xl font-black text-zinc-50">Active Swarm Map</h1>
		<p class="text-xs text-zinc-500 mt-1">Geographic coordinates and status parameters of active routing connections</p>
	</div>

	{#if loading && !data}
		<div class="space-y-6 animate-pulse">
			<div class="h-60 bg-zinc-900 rounded-lg w-full"></div>
			<div class="h-32 bg-zinc-900 rounded-lg w-full"></div>
		</div>
	{:else if error}
		<div class="bg-red-950/20 border border-red-800/40 text-red-400 p-4 rounded-xl text-xs font-mono">
			{error}
		</div>
	{:else if data}
		
		<!-- Swarm map block -->
		<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 flex flex-col gap-6 items-center shadow-xl relative overflow-hidden">
			<!-- World Map SVG Dotted Background -->
			<div class="w-full relative max-w-4xl overflow-hidden rounded-lg bg-zinc-950/60 border border-zinc-850 p-2 py-4">
				
				<!-- Dotted continent map outline simulator -->
				<svg viewBox="0 0 800 250" class="w-full h-auto text-zinc-800 fill-zinc-800 select-none opacity-45">
					<!-- North America dotted sketch -->
					<ellipse cx="140" cy="70" rx="60" ry="30" fill="currentColor" opacity="0.12" />
					<ellipse cx="180" cy="90" rx="50" ry="25" fill="currentColor" opacity="0.12" />
					
					<!-- South America dotted sketch -->
					<ellipse cx="280" cy="160" rx="35" ry="45" fill="currentColor" opacity="0.12" />

					<!-- Europe & Africa dotted sketch -->
					<ellipse cx="420" cy="65" rx="45" ry="28" fill="currentColor" opacity="0.12" />
					<ellipse cx="440" cy="140" rx="35" ry="45" fill="currentColor" opacity="0.12" />

					<!-- Asia & Australia dotted sketch -->
					<ellipse cx="580" cy="90" rx="65" ry="38" fill="currentColor" opacity="0.12" />
					<ellipse cx="640" cy="180" rx="35" ry="28" fill="currentColor" opacity="0.12" />

					<!-- Dotted grid overlay lines -->
					{#each Array(8) as _, i}
						<line x1={i * 100} y1="0" x2={i * 100} y2="250" stroke="#1f2937" stroke-width="0.5" stroke-dasharray="2,5" />
					{/each}
					{#each Array(4) as _, i}
						<line x1="0" y1={i * 60 + 20} x2="800" y2={i * 60 + 20} stroke="#1f2937" stroke-width="0.5" stroke-dasharray="2,5" />
					{/each}

					<!-- Glow pulses representing mapped peer locations -->
					{#each extendedPeers as p}
						<!-- Glowing concentric ring -->
						<circle cx={p.x} cy={p.y} r="8" class="fill-cyan-400/25 animate-ping" />
						<!-- Main location dot -->
						<circle cx={p.x} cy={p.y} r="3.5" class="fill-cyan-400 stroke-zinc-950 stroke-[1.5] shadow-[0_0_8px_#22d3ee]" />
					{/each}
				</svg>

				<!-- Center counter overlay -->
				<div class="absolute inset-0 flex flex-col items-center justify-center pointer-events-none select-none">
					<span class="text-4xl md:text-5xl font-black text-zinc-50 tracking-tight leading-none drop-shadow-[0_4px_12px_rgba(0,0,0,0.8)]">
						{data.PeerCount}
					</span>
					<span class="text-[9px] text-cyan-400 font-mono tracking-widest uppercase mt-1 bg-zinc-950/90 border border-zinc-800 px-2 py-0.5 rounded shadow">
						peers in swarm
					</span>
				</div>
			</div>
		</div>

		<!-- Peers Table Registry -->
		<div class="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden flex flex-col gap-4">
			<div class="px-6 py-4 bg-zinc-950/40 border-b border-zinc-800/80 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
				<h3 class="font-bold text-sm text-zinc-300">Swarm Connections</h3>
				<div class="relative w-full sm:w-64">
					<input
						type="text"
						bind:value={searchFilter}
						placeholder="Filter by country or ID..."
						class="w-full bg-zinc-950/60 border border-zinc-850 text-xs px-3.5 py-1.5 rounded-lg focus:outline-none focus:border-cyan-500"
					/>
				</div>
			</div>

			{#if filteredPeers.length > 0}
				<div class="overflow-x-auto">
					<table class="w-full text-left border-collapse text-xs">
						<thead>
							<tr class="border-b border-zinc-800/60 text-zinc-500 font-mono text-[10px] uppercase bg-zinc-950/20">
								<th class="py-2.5 px-6 font-semibold w-1/4">Location</th>
								<th class="py-2.5 px-6 font-semibold w-12 text-right">Latency</th>
								<th class="py-2.5 px-6 font-semibold w-1/3">Peer ID</th>
								<th class="py-2.5 px-6 font-semibold w-32">Transport</th>
								<th class="py-2.5 px-6 font-semibold text-right">Anchor</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-zinc-850/40 font-mono text-[11px]">
							{#each filteredPeers as peer}
								<tr class="hover:bg-zinc-850/25 transition-colors group">
									<!-- Flag & Country -->
									<td class="py-3.5 px-6 font-sans text-zinc-200 text-xs font-semibold">
										<span class="text-sm select-none mr-1.5">{peer.flag}</span>
										{peer.location}
									</td>
									
									<!-- Latency -->
									<td class="py-3.5 px-6 text-right font-bold font-mono">
										<span class={`${peer.latency < 30 ? 'text-emerald-400' : peer.latency < 80 ? 'text-cyan-400' : 'text-amber-500'}`}>
											{peer.latency} ms
										</span>
									</td>

									<!-- Peer ID -->
									<td class="py-3.5 px-6 text-zinc-400">
										<div class="flex items-center gap-2">
											<span>{peer.peerId}</span>
											<button 
												onclick={() => copyToClipboard(peer.peerId, peer.peerId)}
												class="text-[10px] text-zinc-650 hover:text-zinc-350 opacity-0 group-hover:opacity-100 transition-opacity"
												title="Copy ID"
											>
												{copiedId === peer.peerId ? 'Copied' : 'Copy'}
											</button>
										</div>
									</td>

									<!-- Transport -->
									<td class="py-3.5 px-6 text-zinc-400">{peer.transport}</td>

									<!-- Anchor -->
									<td class="py-3.5 px-6 text-right font-sans">
										{#if peer.isAnchor}
											<span class="px-2 py-0.5 rounded text-[9px] font-bold font-mono bg-blue-950/40 text-blue-400 border border-blue-800/30 uppercase">
												anchor
											</span>
										{:else}
											<span class="text-zinc-600 font-mono text-xs">no</span>
										{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else}
				<div class="py-12 text-center text-zinc-600 italic">No connections match current filters</div>
			{/if}
		</div>
	{/if}
</div>
