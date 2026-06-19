<script lang="ts">
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import { onMount } from 'svelte';
	import { apiFetch } from '$lib/api';

	let { children } = $props();

	let searchQuery = $state('');
	let stats = $state<{ peerCount: number; storeBytes: number } | null>(null);

	// Navigation items list matching the new IPFS WebUI structure
	const navItems = [
		{ name: 'Status', path: '/', icon: '📊' },
		{ name: 'Files', path: '/files', icon: '📁' },
		{ name: 'Explore', path: '/explore', icon: '🌳' },
		{ name: 'Peers', path: '/peers', icon: '🌐' },
		{ name: 'Node Info', path: '/node', icon: '⚙️' }
	];

	function handleSearch(e: Event) {
		e.preventDefault();
		let q = searchQuery.trim();
		if (!q) return;

		q = q.replace(/\s+/g, '');

		if (q.startsWith('/memns/') || q.startsWith('k51')) {
			const name = q.replace('/memns/', '');
			goto(`${base}/memns/${name}`);
		} else if (q.includes('.') && !q.startsWith('mem1')) {
			goto(`${base}/memlink/${q}`);
		} else {
			const midVal = q.replace('/mem/', '');
			goto(`${base}/mid/${midVal}`);
		}
		searchQuery = '';
	}

	onMount(() => {
		// Single initial fetch only — live updates come from the WS on the dashboard page.
		apiFetch('/').then((data) => {
			stats = { peerCount: data.PeerCount || 0, storeBytes: data.StoreBytes || 0 };
		}).catch(() => {});
	});
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<title>Membuss Explorer</title>
</svelte:head>

<div class="min-h-screen bg-zinc-950 text-zinc-100 flex flex-col font-sans selection:bg-cyan-500/30 selection:text-cyan-200">
	<!-- Top Bar -->
	<header class="sticky top-0 z-40 bg-zinc-950/80 backdrop-blur-md border-b border-zinc-900/80 px-4 md:px-8 py-3.5 flex items-center justify-between">
		<div class="flex items-center gap-8">
			<!-- Logo / Brand -->
			<a href={`${base}/`} class="flex items-center gap-2 group">
				<div class="w-8 h-8 rounded-lg bg-gradient-to-tr from-cyan-500 via-blue-600 to-indigo-600 flex items-center justify-center font-bold text-white shadow-[0_0_15px_rgba(6,182,212,0.4)] group-hover:scale-105 transition-transform duration-300">
					M
				</div>
				<div class="flex flex-col">
					<span class="font-bold text-base leading-none tracking-tight bg-gradient-to-r from-zinc-50 to-zinc-300 bg-clip-text text-transparent">Membuss</span>
					<span class="text-[9px] text-cyan-400 font-mono tracking-widest uppercase mt-0.5">Merkle client</span>
				</div>
			</a>

			<!-- Nav links -->
			<nav class="hidden md:flex items-center gap-1">
				{#each navItems as item}
					{@const isActive = item.path === '/' 
						? page.url.pathname === `${base}` || page.url.pathname === `${base}/`
						: page.url.pathname.startsWith(`${base}${item.path}`)}
					<a
						href={`${base}${item.path}`}
						class={`px-3.5 py-1.5 rounded-lg text-xs font-semibold tracking-wide transition-all duration-200 flex items-center gap-1.5 ${
							isActive
								? 'bg-zinc-900 text-cyan-400 border border-zinc-800'
								: 'text-zinc-400 hover:text-zinc-200 hover:bg-zinc-900/40'
						}`}
					>
						<span>{item.icon}</span>
						<span>{item.name}</span>
					</a>
				{/each}
			</nav>
		</div>

		<!-- Search & System Status -->
		<div class="flex items-center gap-4">
			<form onsubmit={handleSearch} class="relative hidden sm:block">
				<input
					type="text"
					bind:value={searchQuery}
					placeholder="Jump to MID, MemNS, or domain..."
					class="w-64 lg:w-80 bg-zinc-900/60 border border-zinc-850 text-zinc-200 placeholder-zinc-500 text-xs px-3.5 py-2 rounded-lg focus:outline-none focus:border-cyan-500/80 focus:ring-1 focus:ring-cyan-500/20 font-mono transition-all duration-300"
				/>
				<button type="submit" class="absolute right-2.5 top-2 text-zinc-500 hover:text-zinc-300">
					<svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
						<path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
					</svg>
				</button>
			</form>

			<!-- Quick Node Status Info -->
			<div class="flex items-center gap-2 px-3 py-1.5 bg-zinc-900/40 rounded-lg border border-zinc-850 text-[10px] font-mono text-zinc-400">
				<span class="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_8px_#10b981]"></span>
				<span>Swarm:</span>
				<span class="text-zinc-200 font-bold">{stats ? stats.peerCount : '--'} connected</span>
			</div>
		</div>
	</header>

	<!-- Mobile Navigation Bar -->
	<div class="md:hidden border-b border-zinc-900 bg-zinc-950 px-4 py-2 flex items-center justify-around gap-1 overflow-x-auto">
		{#each navItems as item}
			{@const isActive = item.path === '/' 
				? page.url.pathname === `${base}` || page.url.pathname === `${base}/`
				: page.url.pathname.startsWith(`${base}${item.path}`)}
			<a
				href={`${base}${item.path}`}
				class={`px-3 py-1.5 rounded-lg text-xs font-semibold whitespace-nowrap transition-all duration-200 flex items-center gap-1 ${
					isActive
						? 'bg-zinc-900 text-cyan-400 border border-zinc-800'
						: 'text-zinc-400 hover:text-zinc-200'
				}`}
			>
				<span>{item.icon}</span>
				<span>{item.name}</span>
			</a>
		{/each}
	</div>

	<!-- Mobile Search Bar -->
	<div class="sm:hidden border-b border-zinc-900 bg-zinc-950/40 p-3">
		<form onsubmit={handleSearch} class="relative">
			<input
				type="text"
				bind:value={searchQuery}
				placeholder="Jump to MID, MemNS, or domain..."
				class="w-full bg-zinc-900/60 border border-zinc-850 text-zinc-200 placeholder-zinc-500 text-xs px-3.5 py-2.5 rounded-lg focus:outline-none focus:border-cyan-500 font-mono"
			/>
			<button type="submit" class="absolute right-3 top-2.5 text-zinc-500">
				<svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
					<path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
				</svg>
			</button>
		</form>
	</div>

	<!-- Main Content Area -->
	<main class="flex-grow max-w-7xl w-full mx-auto p-4 md:p-8 flex flex-col gap-8">
		{@render children()}
	</main>

	<!-- Footer -->
	<footer class="border-t border-zinc-900 bg-zinc-950/40 py-6 px-4 md:px-8 text-center text-xs text-zinc-650 font-mono flex flex-col sm:flex-row items-center justify-between gap-4">
		<div>
			Membuss Decentralized Network &copy; {new Date().getFullYear()}
		</div>
		<div class="flex items-center gap-2 text-zinc-555">
			<span class="w-1.5 h-1.5 rounded-full bg-cyan-500"></span>
			<span>Served by Mem-Gate Public Proxy Layer</span>
		</div>
	</footer>
</div>
