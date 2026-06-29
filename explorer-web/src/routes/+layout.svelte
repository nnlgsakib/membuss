<script lang="ts">
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import { onMount } from 'svelte';
	import { apiFetch } from '$lib/api';
	import Icon from '@iconify/svelte';

	let { children } = $props();

	let searchQuery = $state('');
	let stats = $state<{ peerCount: number; storeBytes: number } | null>(null);

	const navItems = [
		{ name: 'Status', path: '/', icon: 'ph:gauge' },
		{ name: 'Files', path: '/files', icon: 'ph:folder-open' },
		{ name: 'MemNS', path: '/memns', icon: 'ph:identification-card' },
		{ name: 'Explore', path: '/explore', icon: 'ph:git-branch' },
		{ name: 'Peers', path: '/peers', icon: 'ph:circle-notch' },
		{ name: 'Node Info', path: '/node', icon: 'ph:gear-six' }
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
		apiFetch('/').then((data) => {
			stats = { peerCount: data.PeerCount || 0, storeBytes: data.StoreBytes || 0 };
		}).catch(() => {});
	});
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<title>Membuss Explorer</title>
</svelte:head>

<div class="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans selection:bg-cyan-500/30 selection:text-cyan-200">
	<!-- Top Bar -->
	<header class="sticky top-0 z-40 bg-slate-950/80 backdrop-blur-md border-b border-slate-800/80 px-4 md:px-8 py-3.5 flex items-center justify-between">
		<div class="flex items-center gap-8">
			<!-- Logo / Brand -->
			<a href={`${base}/`} class="flex items-center gap-2.5 group">
				<div class="w-8 h-8 rounded-lg bg-cyan-500 flex items-center justify-center font-bold text-slate-950 text-sm group-hover:scale-105 transition-transform duration-200">
					M
				</div>
				<div class="flex flex-col">
					<span class="font-bold text-base leading-none tracking-tight text-slate-100">Membuss</span>
					<span class="text-[9px] text-slate-500 font-mono tracking-widest mt-0.5">distributed storage</span>
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
						class={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-all duration-200 flex items-center gap-1.5 ${
							isActive
								? 'bg-slate-800/60 text-cyan-400'
								: 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/40'
						}`}
					>
						<Icon icon={item.icon} class="w-4 h-4" />
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
					class="w-64 lg:w-80 bg-slate-800/60 border border-slate-700/50 text-slate-200 placeholder-slate-500 text-xs px-3.5 py-2 rounded-lg focus:outline-none focus:border-slate-500 focus:ring-1 focus:ring-slate-500/20 font-mono transition-all duration-200"
				/>
				<button type="submit" class="absolute right-2.5 top-2 text-slate-500 hover:text-slate-300">
					<Icon icon="ph:magnifying-glass" class="w-4 h-4" />
				</button>
			</form>

			<!-- Quick Node Status Info -->
			<div class="flex items-center gap-2 px-3 py-1.5 bg-slate-800/50 rounded-lg border border-slate-700/50 text-[10px] font-mono text-slate-400">
				<Icon icon="ph:circle-fill" class="w-2 h-2 text-emerald-500" />
				<span>Swarm:</span>
				<span class="text-slate-200 font-bold">{stats ? stats.peerCount : '--'} connected</span>
			</div>
		</div>
	</header>

	<!-- Mobile Navigation Bar -->
	<div class="md:hidden border-b border-slate-800 bg-slate-950 px-4 py-2 flex items-center justify-around gap-1 overflow-x-auto">
		{#each navItems as item}
			{@const isActive = item.path === '/'
				? page.url.pathname === `${base}` || page.url.pathname === `${base}/`
				: page.url.pathname.startsWith(`${base}${item.path}`)}
			<a
				href={`${base}${item.path}`}
				class={`px-3 py-1.5 rounded-lg text-xs font-semibold whitespace-nowrap transition-all duration-200 flex items-center gap-1 ${
					isActive
						? 'bg-slate-800/60 text-cyan-400'
						: 'text-slate-400 hover:text-slate-200'
				}`}
			>
				<Icon icon={item.icon} class="w-4 h-4" />
				<span>{item.name}</span>
			</a>
		{/each}
	</div>

	<!-- Mobile Search Bar -->
	<div class="sm:hidden border-b border-slate-800 bg-slate-950/40 p-3">
		<form onsubmit={handleSearch} class="relative">
			<input
				type="text"
				bind:value={searchQuery}
				placeholder="Jump to MID, MemNS, or domain..."
				class="w-full bg-slate-800/60 border border-slate-700/50 text-slate-200 placeholder-slate-500 text-xs px-3.5 py-2.5 rounded-lg focus:outline-none focus:border-slate-500 font-mono"
			/>
			<button type="submit" class="absolute right-3 top-2.5 text-slate-500">
				<Icon icon="ph:magnifying-glass" class="w-4 h-4" />
			</button>
		</form>
	</div>

	<!-- Main Content Area -->
	<main class="flex-grow max-w-7xl w-full mx-auto p-4 md:p-8 flex flex-col gap-8">
		{@render children()}
	</main>

	<!-- Footer -->
	<footer class="border-t border-slate-800 bg-slate-950/40 py-6 px-4 md:px-8 text-center text-xs text-slate-600 font-mono flex flex-col sm:flex-row items-center justify-between gap-4">
		<div>
			Membuss Decentralized Network &copy; {new Date().getFullYear()}
		</div>
		<div class="flex items-center gap-2 text-slate-500">
			<Icon icon="ph:circle-fill" class="w-2 h-2 text-cyan-500" />
			<span>Served by Mem-Gate Public Proxy Layer</span>
		</div>
	</footer>
</div>
