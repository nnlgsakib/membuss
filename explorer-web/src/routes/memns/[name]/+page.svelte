<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { apiFetch } from '$lib/api';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';

	interface MemRoute {
		target: string;
		weight: number;
		label: string;
	}

	interface MemLogEntry {
		sequence: number;
		value: string;
		timestamp: string;
		message: string;
	}

	interface MemNSData {
		Title: string;
		Name: string;
		Value: string;
		CleanValue: string;
		IsMID: boolean;
		Sequence: number;
		ExpiresAt: string;
		TTL: string;
		Routes: MemRoute[] | null;
		Delegates: string[] | null;
		Changelog: MemLogEntry[] | null;
	}

	let data = $state<MemNSData | null>(null);
	let loading = $state(true);
	let error = $state<string | null>(null);

	async function loadRecord() {
		try {
			const name = page.params.name;
			const res = await apiFetch(`/memns/${name}`);
			data = res;
			loading = false;
		} catch (err) {
			error = err instanceof Error ? err.message : 'Failed to resolve MemNS record';
			loading = false;
		}
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
		loadRecord();
	});
</script>

<div class="flex flex-col gap-6">
	<!-- Page Header -->
	<div class="border-b border-slate-800 pb-4 flex items-center justify-between">
		<div>
			<div class="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded bg-amber-950/60 border border-amber-800/40 text-[10px] text-amber-400 font-mono tracking-wider uppercase">
				Mutable Target (MemNS)
			</div>
			<h1 class="text-2xl font-bold text-slate-50 mt-1 break-all select-all">/{page.params.name}</h1>
		</div>
	</div>

	{#if loading && !data}
		<div class="space-y-6 animate-pulse">
			<div class="h-40 bg-slate-900 rounded-lg"></div>
			<div class="h-32 bg-slate-900 rounded-lg"></div>
		</div>
	{:else if error}
		<div class="bg-red-950/20 border border-red-800/40 text-red-400 p-4 rounded-xl text-xs font-mono">
			{error}
		</div>
	{:else if data}
		<div class="grid grid-cols-1 gap-6">
			<!-- Details Panel -->
			<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4">
			<h3 class="font-bold text-sm text-slate-400 font-mono border-b border-slate-800 pb-2">
				Resolution Parameters
			</h3>
				<dl class="grid grid-cols-1 md:grid-cols-4 gap-4 text-xs font-mono">
					<div class="flex flex-col gap-1 md:col-span-4 bg-slate-950/40 border border-slate-750 p-3 rounded-lg">
						<span class="text-slate-500 uppercase text-[9px]">Resolved Value</span>
						{#if data.IsMID}
							<a 
								href={`${base}/mid/${data.CleanValue}`} 
								class="text-cyan-400 text-sm font-bold hover:underline break-all"
							>
								{data.Value}
							</a>
						{:else}
							<span class="text-slate-300 text-sm font-bold break-all">{data.Value}</span>
						{/if}
					</div>

					<div class="flex flex-col gap-1">
						<span class="text-slate-500 uppercase text-[9px]">Sequence Number</span>
						<span class="text-slate-200 text-sm font-bold">{data.Sequence}</span>
					</div>

					<div class="flex flex-col gap-1">
						<span class="text-slate-500 uppercase text-[9px]">Remaining TTL</span>
						<span class="text-slate-200 text-sm font-bold">{data.TTL}</span>
					</div>

					<div class="flex flex-col gap-1 md:col-span-2">
						<span class="text-slate-500 uppercase text-[9px]">Record Expiration</span>
						<span class="text-slate-200 text-sm font-bold">{formatDate(data.ExpiresAt)}</span>
					</div>
				</dl>
			</div>

			<!-- Routing Rules -->
			{#if data.Routes && data.Routes.length > 0}
				<div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">
					<div class="px-6 py-4 bg-slate-950/40 border-b border-slate-800">
						<h3 class="font-bold text-sm text-slate-300">Weighted Routing Rule Engine</h3>
					</div>
					<div class="overflow-x-auto">
						<table class="w-full text-left border-collapse text-sm">
							<thead>
								<tr class="border-b border-slate-800/60 text-slate-500 font-mono text-xs uppercase bg-slate-950/20">
									<th class="py-3 px-6 font-semibold">Route Label</th>
									<th class="py-3 px-6 font-semibold">Destination Target MID</th>
									<th class="py-3 px-6 font-semibold w-24 text-right">Weight</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-slate-750/40 font-mono text-xs">
								{#each data.Routes as r}
									<tr class="hover:bg-slate-750/25 transition-colors">
										<td class="py-3.5 px-6 font-semibold text-slate-200">{r.label || 'n/a'}</td>
										<td class="py-3.5 px-6">
											<a href={`${base}/mid/${r.target}`} class="text-cyan-400 hover:underline">
												{r.target}
											</a>
										</td>
										<td class="py-3.5 px-6 text-slate-300 text-right font-bold">{r.weight}%</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/if}

			<!-- Delegates -->
			{#if data.Delegates && data.Delegates.length > 0}
				<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4">
					<h3 class="font-bold text-sm text-slate-400 font-mono border-b border-slate-800 pb-2">
						Delegated Signing Keys
					</h3>
					<ul class="flex flex-col gap-2 font-mono text-xs text-slate-300">
						{#each data.Delegates as key}
							<li class="bg-slate-950/60 border border-slate-750 px-4 py-2.5 rounded-lg select-all break-all">
								{key}
							</li>
						{/each}
					</ul>
				</div>
			{/if}

			<!-- Publish History Timeline -->
			<div class="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden">
				<div class="px-6 py-4 bg-slate-950/40 border-b border-slate-800">
					<h3 class="font-bold text-sm text-slate-300">Publish History Timeline (MemLog)</h3>
				</div>
				
				{#if data.Changelog && data.Changelog.length > 0}
					<div class="overflow-x-auto">
						<table class="w-full text-left border-collapse text-sm">
							<thead>
								<tr class="border-b border-slate-800/60 text-slate-500 font-mono text-xs uppercase bg-slate-950/20">
									<th class="py-3 px-6 font-semibold w-16">Seq</th>
									<th class="py-3 px-6 font-semibold w-48">Timestamp</th>
									<th class="py-3 px-6 font-semibold">Value / Route Target</th>
									<th class="py-3 px-6 font-semibold text-right">Commit Message</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-slate-750/40 font-mono text-xs">
								{#each data.Changelog as entry}
									<tr class="hover:bg-slate-750/25 transition-colors">
										<td class="py-3.5 px-6 text-slate-400 font-bold">{entry.sequence}</td>
										<td class="py-3.5 px-6 text-slate-500">{formatDate(entry.timestamp)}</td>
										<td class="py-3.5 px-6 break-all max-w-sm">
											{#if entry.value.startsWith('/mem/') || entry.value.startsWith('mem1')}
												{@const cleanVal = entry.value.replace('/mem/', '')}
												<a href={`${base}/mid/${cleanVal}`} class="text-cyan-400 hover:underline">
													{entry.value}
												</a>
											{:else}
												<span class="text-slate-350">{entry.value}</span>
											{/if}
										</td>
										<td class="py-3.5 px-6 text-slate-400 text-right font-sans italic">
											{entry.message || '—'}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{:else}
					<div class="py-8 text-center text-slate-550 flex flex-col items-center justify-center gap-1.5">
						<Icon icon="ph:clock-counter-clockwise" class="text-3xl text-slate-600" />
						<p class="text-xs text-slate-500">No timeline events recorded for this record</p>
					</div>
				{/if}
			</div>
		</div>
	{/if}
</div>
