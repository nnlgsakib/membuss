<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { tick } from 'svelte';
	import { apiFetch } from '$lib/api';
	import L from 'leaflet';
	import 'leaflet/dist/leaflet.css';

	interface PeerInfo {
		PeerID: string;
		Addrs: string[];
		IsAnchor: boolean;
		Connected: boolean;
		Country: string;
		City: string;
		Lat: number;
		Lon: number;
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
	let searchFilter = $state('');
	let mapEl = $state<HTMLDivElement>();
	let map: L.Map | undefined;
	let markers: L.Marker[] = [];
	let mapReady = $state(false);

	interface DisplayPeer {
		peerId: string;
		addrs: string[];
		isAnchor: boolean;
		connected: boolean;
		location: string;
		lat: number;
		lon: number;
		transport: string;
	}

	let displayPeers = $state<DisplayPeer[]>([]);

	function initMap() {
		if (map || !mapEl) return;
		map = L.map(mapEl, {
			center: [20, 0],
			zoom: 2,
			minZoom: 2,
			maxZoom: 12,
			zoomControl: false,
			attributionControl: false,
			scrollWheelZoom: true,
			maxBounds: [[-85, -360], [85, 360]],
			maxBoundsViscosity: 1.0
		});
		L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
			subdomains: 'abcd',
			maxZoom: 19
		}).addTo(map);
		L.control.zoom({ position: 'topright' }).addTo(map);
		mapReady = true;
	}

	function updateMapMarkers() {
		if (!map) return;
		markers.forEach((m) => m.remove());
		markers = [];

		const validPeers = displayPeers.filter((p) => p.lat !== 0 || p.lon !== 0);

		validPeers.forEach((p) => {
			const icon = L.divIcon({
				className: 'peer-marker',
				html: `<div class="w-3 h-3 rounded-full ${p.isAnchor ? 'bg-blue-500' : 'bg-emerald-500'} shadow-lg animate-pulse"></div>`,
				iconSize: [12, 12],
				iconAnchor: [6, 6]
			});
			const marker = L.marker([p.lat, p.lon], { icon })
				.bindPopup(
					`<div style="font-family:monospace;font-size:12px;padding:4px">
						<div style="font-weight:bold">${p.peerId}</div>
						<div style="color:#94a3b8">${p.location}</div>
						<div style="color:#94a3b8">${p.transport}</div>
						${p.isAnchor ? '<div style="color:#60a5fa;font-weight:bold">Anchor</div>' : ''}
					</div>`
				)
				.addTo(map);
			markers.push(marker);
		});

		if (validPeers.length > 0) {
			const bounds = L.latLngBounds(validPeers.map((p) => [p.lat, p.lon] as [number, number]));
			map.fitBounds(bounds, { padding: [40, 40], maxZoom: 6 });
		}
	}

	async function loadPeers() {
		try {
			const res = await apiFetch('/peers');
			data = res;

			if (data && data.Peers) {
				displayPeers = data.Peers.map((p) => {
					let transport = 'QUIC (UDP)';
					if (p.Addrs && p.Addrs.length > 0) {
						if (p.Addrs[0].includes('/tcp/')) transport = 'TCP';
						else if (p.Addrs[0].includes('/ws/')) transport = 'WebSockets';
					}

					const location = [p.City, p.Country].filter(Boolean).join(', ') || 'Unknown';

					return {
						peerId: p.PeerID,
						addrs: p.Addrs,
						isAnchor: p.IsAnchor,
						connected: p.Connected,
						location,
						lat: p.Lat,
						lon: p.Lon,
						transport
					};
				});
			}
			loading = false;

			// Wait for DOM to update, then init map + markers
			await tick();
			initMap();
			updateMapMarkers();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to query peer swarm';
			loading = false;
		}
	}

	let filteredPeers = $derived.by(() => {
		return displayPeers.filter((p) => {
			return (
				!searchFilter ||
				p.peerId.toLowerCase().includes(searchFilter.toLowerCase()) ||
				p.location.toLowerCase().includes(searchFilter.toLowerCase()) ||
				p.transport.toLowerCase().includes(searchFilter.toLowerCase())
			);
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
		const interval = setInterval(() => {
			loadPeers();
		}, 10000);
		return () => clearInterval(interval);
	});

	onDestroy(() => {
		if (map) {
			map.remove();
			map = undefined;
		}
	});
</script>

<div class="flex flex-col gap-6">
	<!-- Page Header -->
	<div class="border-b border-slate-800 pb-4">
		<h1 class="text-2xl font-bold text-slate-50">Active Swarm Map</h1>
		<p class="text-xs text-slate-500 mt-1">
			Geographic coordinates and status parameters of active routing connections
		</p>
	</div>

	{#if loading && !data}
		<div class="space-y-6 animate-pulse">
			<div class="h-60 bg-slate-900 rounded-lg w-full"></div>
			<div class="h-32 bg-slate-900 rounded-lg w-full"></div>
		</div>
	{:else if error}
		<div
			class="bg-red-950/20 border border-red-800/40 text-red-400 p-4 rounded-xl text-xs font-mono"
		>
			{error}
		</div>
	{:else if data}
		<!-- Leaflet World Map -->
		<div
			class="bg-slate-900 border border-slate-800 rounded-xl shadow-xl relative overflow-hidden"
		>
			<!-- Peer counter overlay -->
			<div
				class="absolute top-4 left-4 z-[1000] flex flex-col items-start pointer-events-none select-none"
			>
				<span
					class="text-3xl md:text-4xl font-bold text-slate-50 tracking-tight leading-none"
				>
					{data.PeerCount}
				</span>
				<span
					class="text-[9px] text-emerald-500 font-mono tracking-widest uppercase mt-1 bg-slate-950/90 border border-slate-800 px-2 py-0.5 rounded shadow"
				>
					peers in swarm
				</span>
			</div>
			<!-- Leaflet map container -->
			<div bind:this={mapEl} class="h-[400px] w-full rounded-xl"></div>
		</div>

		<!-- Peers Table Registry -->
		<div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden flex flex-col gap-4">
			<div
				class="px-6 py-4 bg-slate-950/40 border-b border-slate-800/80 flex flex-col sm:flex-row sm:items-center justify-between gap-4"
			>
				<h3 class="font-bold text-sm text-slate-300">Swarm Connections</h3>
				<div class="relative w-full sm:w-64">
					<input
						type="text"
						bind:value={searchFilter}
						placeholder="Filter by country or ID..."
						class="w-full bg-slate-950/60 border border-slate-850 text-xs px-3.5 py-1.5 rounded-lg focus:outline-none focus:border-emerald-500"
					/>
				</div>
			</div>

			{#if filteredPeers.length > 0}
				<div class="overflow-x-auto">
					<table class="w-full text-left border-collapse text-xs">
						<thead>
							<tr
								class="border-b border-slate-800/60 text-slate-500 font-mono text-[10px] uppercase bg-slate-950/20"
							>
								<th class="py-2.5 px-6 font-semibold w-1/4">Location</th>
								<th class="py-2.5 px-6 font-semibold w-1/3">Peer ID</th>
								<th class="py-2.5 px-6 font-semibold w-32">Transport</th>
								<th class="py-2.5 px-6 font-semibold text-right">Anchor</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-850/40 font-mono text-[11px]">
							{#each filteredPeers as peer}
								<tr class="hover:bg-slate-850/25 transition-colors group">
									<td
										class="py-3.5 px-6 font-sans text-slate-200 text-xs font-semibold"
									>
										{peer.location}
									</td>

									<td class="py-3.5 px-6 text-slate-400">
										<div class="flex items-center gap-2">
											<span>{peer.peerId}</span>
											<button
												onclick={() => copyToClipboard(peer.peerId, peer.peerId)}
												class="text-[10px] text-slate-650 hover:text-slate-350 opacity-0 group-hover:opacity-100 transition-opacity"
												title="Copy ID"
											>
												{copiedId === peer.peerId ? 'Copied' : 'Copy'}
											</button>
										</div>
									</td>

									<td class="py-3.5 px-6 text-slate-400">{peer.transport}</td>

									<td class="py-3.5 px-6 text-right font-sans">
										{#if peer.isAnchor}
											<span
												class="px-2 py-0.5 rounded text-[9px] font-bold font-mono bg-blue-950/40 text-blue-400 border border-blue-800/30 uppercase"
											>
												anchor
											</span>
										{:else}
											<span class="text-slate-600 font-mono text-xs">no</span>
										{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else}
				<div class="py-12 text-center text-slate-600 italic">
					No connections match current filters
				</div>
			{/if}
		</div>
	{/if}
</div>

<style>
	:global(.peer-marker) {
		background: none !important;
		border: none !important;
	}
	:global(.leaflet-popup-content-wrapper) {
		background: rgba(15, 23, 42, 0.95) !important;
		color: #e2e8f0 !important;
		border: 1px solid #1e293b !important;
		border-radius: 0.5rem !important;
		box-shadow: 0 4px 20px rgba(0, 0, 0, 0.5) !important;
	}
	:global(.leaflet-popup-tip) {
		background: rgba(15, 23, 42, 0.95) !important;
		border: 1px solid #1e293b !important;
	}
	:global(.leaflet-control-zoom a) {
		background: rgba(15, 23, 42, 0.9) !important;
		color: #94a3b8 !important;
		border-color: #1e293b !important;
	}
	:global(.leaflet-control-zoom a:hover) {
		background: rgba(30, 41, 59, 0.9) !important;
		color: #e2e8f0 !important;
	}
</style>
