<script lang="ts">
	import { onMount } from 'svelte';
	import { base } from '$app/paths';
	import { formatBytes } from '$lib/api';
	import DagNode from './DagNode.svelte';

	interface LinkNode {
		size: number | null;
		links: string[] | null;
	}

	let { mid, depth = 0 }: { mid: string; depth?: number } = $props();

	let expanded = $state(false);
	let loading = $state(false);
	let error = $state<string | null>(null);
	let nodeData = $state<LinkNode | null>(null);

	async function toggleNode() {
		if (expanded) {
			expanded = false;
			return;
		}

		expanded = true;
		if (nodeData) return; // already loaded

		loading = true;
		error = null;
		try {
			const res = await fetch(`${base.replace('/explorer', '')}/mem/${encodeURIComponent(mid)}?format=dag-json`);
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			nodeData = await res.json();
		} catch (err) {
			error = err instanceof Error ? err.message : 'Fetch failed';
		} finally {
			loading = false;
		}
	}
</script>

<div class="flex flex-col pl-4 border-l border-zinc-800/80 my-1 font-mono text-xs text-zinc-400">
	<!-- Node Line -->
	<div class="flex items-center gap-2 py-1">
		<!-- Expand Toggle Button -->
		<button 
			onclick={toggleNode}
			class="w-5 h-5 rounded hover:bg-zinc-800 flex items-center justify-center font-bold text-[10px] text-zinc-500 hover:text-cyan-400 transition-colors shrink-0"
		>
			{#if loading}
				<span class="w-2.5 h-2.5 border border-cyan-500/30 border-t-cyan-400 rounded-full animate-spin"></span>
			{:else if expanded}
				▼
			{:else}
				▶
			{/if}
		</button>

		<!-- Icon based on type -->
		<span class="text-xs">
			{#if depth === 0}
				🌳
			{:else if nodeData && (!nodeData.links || nodeData.links.length === 0)}
				📄
			{:else}
				🔗
			{/if}
		</span>

		<!-- MID string -->
		<a 
			href={`${base}/mid/${mid}`} 
			class="hover:text-cyan-400 hover:underline tracking-tight truncate max-w-[240px] sm:max-w-md"
		>
			{mid}
		</a>

		<!-- Info badge -->
		{#if nodeData}
			<span class="text-[10px] text-zinc-550 bg-zinc-900 border border-zinc-850 px-1.5 py-0.5 rounded font-sans scale-95 shrink-0">
				{#if nodeData.size !== null}
					{formatBytes(nodeData.size)}
				{/if}
				{#if nodeData.links && nodeData.links.length > 0}
					· {nodeData.links.length} link{nodeData.links.length === 1 ? '' : 's'}
				{/if}
			</span>
		{/if}
	</div>

	<!-- Children Links -->
	{#if expanded}
		<div class="flex flex-col ml-2">
			{#if loading && !nodeData}
				<div class="py-1 pl-6 text-zinc-600 italic text-[11px]">Loading DAG block data...</div>
			{:else if error}
				<div class="py-1 pl-6 text-red-400 italic text-[11px]">Error: {error}</div>
			{:else if nodeData}
				{#if nodeData.links && nodeData.links.length > 0}
					{#each nodeData.links as childMid}
						<DagNode mid={childMid} depth={depth + 1} />
					{/each}
				{:else}
					<div class="py-1 pl-6 text-zinc-600 italic text-[10px] tracking-wide select-none">
						(leaf block — no child nodes)
					</div>
				{/if}
			{/if}
		</div>
	{/if}
</div>
