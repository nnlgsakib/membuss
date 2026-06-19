<script lang="ts">
	import { base } from '$app/paths';
	import DagNode from '$lib/components/DagNode.svelte';
	import Icon from '@iconify/svelte';

	let inputMID = $state('');
	let visualizedMID = $state('');

	function handleVisualize(e: Event) {
		e.preventDefault();
		const q = inputMID.trim().replace('/mem/', '').replace(/\s+/g, '');
		if (!q) return;

		visualizedMID = q;
	}
</script>

<div class="flex flex-col gap-6 max-w-4xl w-full mx-auto">
	<!-- Page Header -->
	<div class="border-b border-slate-800 pb-4">
		<h1 class="text-2xl font-bold text-slate-50">Merkle DAG Visualizer</h1>
		<p class="text-xs text-slate-500 mt-1">Traverse and explore the block tree of any content multihash on the network</p>
	</div>

	<!-- Selector Input card -->
	<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4 shadow-xl">
		<form onsubmit={handleVisualize} class="flex flex-col sm:flex-row gap-3">
			<input
				type="text"
				bind:value={inputMID}
				required
				placeholder="Enter MID hash (e.g. mem1z...)"
				class="flex-grow bg-slate-950/60 border border-slate-850 text-xs px-3.5 py-3 rounded-lg focus:outline-none focus:border-cyan-500/80 focus:ring-1 focus:ring-cyan-500/20 font-mono text-slate-200"
			/>
			<button 
				type="submit"
				class="px-6 py-3 bg-cyan-500 hover:bg-cyan-400 text-slate-950 font-bold text-xs rounded-lg transition-colors shrink-0 active:scale-[0.98] transition-all duration-200"
			>
				Visualize Merkle DAG
			</button>
		</form>
	</div>

	<!-- Visualization Render Box -->
	{#if visualizedMID}
		<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4 shadow-lg">
			<div class="flex flex-col sm:flex-row sm:items-center justify-between border-b border-slate-850 pb-3 gap-2">
				<div class="flex flex-col">
					<span class="text-[9px] font-mono text-slate-500">Visualizing Tree</span>
					<code class="text-xs text-slate-300 font-mono font-bold break-all select-all">{visualizedMID}</code>
				</div>
				<button 
					onclick={() => visualizedMID = ''} 
					class="text-[10px] text-slate-550 hover:text-slate-350 font-mono hover:underline"
				>
					Clear view
				</button>
			</div>

			<!-- DAG Component mount -->
			<div class="p-4 bg-slate-950/30 border border-slate-850 rounded-xl max-h-[550px] overflow-y-auto mt-2">
				<DagNode mid={visualizedMID} depth={0} />
			</div>
		</div>
	{:else}
		<div class="py-20 border-2 border-dashed border-slate-850 rounded-xl flex flex-col items-center justify-center text-center gap-3">
			<Icon icon="ph:tree-structure" class="text-4xl text-slate-600" />
			<div class="text-sm font-semibold text-slate-400 font-sans">No Content Loaded</div>
			<p class="text-xs text-slate-650 max-w-xs">
				Input a valid Membuss Content Identifier (MID) above to unpack, scan, and render its Merkle links hierarchy.
			</p>
		</div>
	{/if}
</div>
