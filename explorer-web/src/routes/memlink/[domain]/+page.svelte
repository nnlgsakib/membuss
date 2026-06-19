<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { apiFetch } from '$lib/api';
	import { base } from '$app/paths';

	interface MemLinkData {
		Title: string;
		Domain: string;
		RawTXT: string;
		ResolvedMemNSName: string;
		ResolvedMID: string;
		TTLRemaining: number;
	}

	let data = $state<MemLinkData | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);

	async function loadLink() {
		try {
			const domain = page.params.domain;
			const res = await apiFetch(`/memlink/${domain}`);
			data = res;
			loading = false;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to query DNS MemLink mapping';
			loading = false;
		}
	}

	onMount(() => {
		loadLink();
	});
</script>

<div class="flex flex-col gap-6">
	<!-- Page Header -->
	<div class="border-b border-zinc-800 pb-4">
		<div class="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded bg-blue-950/60 border border-blue-800/40 text-[10px] text-blue-400 font-mono tracking-wider uppercase">
			DNS Bridge link (MemLink)
		</div>
		<h1 class="text-2xl font-black text-zinc-50 mt-1 select-all">{page.params.domain}</h1>
	</div>

	{#if loading && !data}
		<div class="space-y-4 animate-pulse">
			<div class="h-44 bg-zinc-900 rounded-lg"></div>
		</div>
	{:else if error}
		<div class="bg-red-950/20 border border-red-800/40 text-red-400 p-4 rounded-xl text-xs font-mono">
			{error}
		</div>
	{:else if data}
		<div class="bg-zinc-900 border border-zinc-800 rounded-xl p-6 flex flex-col gap-4">
			<h3 class="font-bold text-sm text-zinc-400 font-mono uppercase tracking-wider border-b border-zinc-800 pb-2">
				Resolution Pipeline Debugger
			</h3>
			<dl class="grid grid-cols-1 md:grid-cols-4 gap-4 text-xs font-mono">
				<!-- Raw DNS Record -->
				<div class="flex flex-col gap-1 md:col-span-4 bg-zinc-950/40 border border-zinc-850 p-3 rounded-lg">
					<span class="text-zinc-500 uppercase text-[9px]">Raw TXT Value</span>
					{#if data.RawTXT}
						<span class="text-zinc-300 font-bold select-all break-all">{data.RawTXT}</span>
					{:else}
						<span class="text-amber-500 font-bold">No active dns TXT records resolved at "_membuss.{data.Domain}"</span>
					{/if}
				</div>

				<!-- MemNS Pointer -->
				<div class="flex flex-col gap-1 md:col-span-2 bg-zinc-950/20 border border-zinc-850/60 p-3 rounded-lg">
					<span class="text-zinc-500 uppercase text-[9px]">Resolved MemNS Pointer</span>
					{#if data.ResolvedMemNSName}
						<a 
							href={`${base}/memns/${data.ResolvedMemNSName.replace('/memns/', '')}`} 
							class="text-cyan-400 font-bold hover:underline break-all mt-1"
						>
							{data.ResolvedMemNSName}
						</a>
					{:else}
						<span class="text-zinc-600 italic mt-1">no mutable name bound</span>
					{/if}
				</div>

				<!-- Resolved Target MID -->
				<div class="flex flex-col gap-1 md:col-span-2 bg-zinc-950/20 border border-zinc-850/60 p-3 rounded-lg">
					<span class="text-zinc-500 uppercase text-[9px]">Resolved Destination MID</span>
					{#if data.ResolvedMID}
						<a 
							href={`${base}/mid/${data.ResolvedMID}`} 
							class="text-cyan-400 font-bold hover:underline break-all mt-1"
						>
							{data.ResolvedMID}
						</a>
					{:else}
						<span class="text-zinc-650 italic mt-1">no direct target content ID</span>
					{/if}
				</div>

				<div class="flex flex-col gap-1 mt-2">
					<span class="text-zinc-500 uppercase text-[9px]">Cache TTL Remaining</span>
					<span class="text-zinc-300 text-sm font-bold">
						{#if data.TTLRemaining >= 0}
							{data.TTLRemaining} seconds
						{:else}
							<span class="text-zinc-600">expired / non-cached</span>
						{/if}
					</span>
				</div>
			</dl>
		</div>
	{/if}
</div>
