<script lang="ts">
	import { onMount } from 'svelte';
	import { apiFetch } from '$lib/api';
	import { base } from '$app/paths';
	import Icon from '@iconify/svelte';

	interface Key {
		name: string;
		type: string;
		pubkey: string;
		memns_name: string;
	}

	interface MemNSResolveResult {
		value: string;
		sequence: number;
		ttl: string;
		expiresAt: string;
	}

	let keys = $state<Key[]>([]);
	let loadingKeys = $state(true);
	let errorKeys = $state<string | null>(null);

	// Key Generation form state
	let newKeyName = $state('');
	let newKeyType = $state('ed25519');
	let generatingKey = $state(false);
	let errorGen = $state<string | null>(null);

	// Publish form state
	let selectedKey = $state('');
	let publishValue = $state('');
	let publishTTL = $state(86400);
	let publishMsg = $state('');
	let publishing = $state(false);
	let successPublish = $state<string | null>(null);
	let errorPublish = $state<string | null>(null);

	// Resolve test state
	let resolveInput = $state('');
	let resolving = $state(false);
	let resolvedResult = $state<MemNSResolveResult | null>(null);
	let errorResolve = $state<string | null>(null);

	// Active Tab inside MemNS management
	let activeTab = $state<'keys' | 'resolve'>('keys');

	async function fetchKeys() {
		try {
			loadingKeys = true;
			const data = await apiFetch('/keyring/list');
			keys = data || [];
			loadingKeys = false;
		} catch (err) {
			errorKeys = err instanceof Error ? err.message : 'Failed to fetch Keyring';
			loadingKeys = false;
		}
	}

	async function handleGenerateKey(e: Event) {
		e.preventDefault();
		const name = newKeyName.trim();
		if (!name) return;

		try {
			generatingKey = true;
			errorGen = null;
			await apiFetch('/keyring/gen', {
				method: 'POST',
				body: JSON.stringify({ name, type: newKeyType })
			});
			newKeyName = '';
			await fetchKeys();
			generatingKey = false;
		} catch (err) {
			errorGen = err instanceof Error ? err.message : 'Failed to generate key';
			generatingKey = false;
		}
	}

	async function handleDeleteKey(name: string) {
		if (!confirm(`Are you sure you want to delete the key "${name}"? This action cannot be undone.`)) {
			return;
		}

		try {
			await apiFetch(`/keyring/rm/${name}`, {
				method: 'DELETE'
			});
			await fetchKeys();
		} catch (err) {
			alert(`Delete failed: ${err instanceof Error ? err.message : err}`);
		}
	}

	async function handlePublish(e: Event) {
		e.preventDefault();
		if (!selectedKey || !publishValue.trim()) return;

		try {
			publishing = true;
			errorPublish = null;
			successPublish = null;
			const res = await apiFetch('/memns/publish', {
				method: 'POST',
				body: JSON.stringify({
					key: selectedKey,
					value: publishValue.trim(),
					ttl: Number(publishTTL),
					message: publishMsg.trim()
				})
			});
			successPublish = `Successfully published record to MemNS: ${res.name} -> ${res.value}`;
			publishValue = '';
			publishMsg = '';
			publishing = false;
		} catch (err) {
			errorPublish = err instanceof Error ? err.message : 'Failed to publish record';
			publishing = false;
		}
	}

	async function handleResolve(e: Event) {
		e.preventDefault();
		const name = resolveInput.trim().replace('/memns/', '');
		if (!name) return;

		try {
			resolving = true;
			errorResolve = null;
			resolvedResult = null;
			const data = await apiFetch(`/memns/${name}`);
			resolvedResult = {
				value: data.Value,
				sequence: data.Sequence,
				ttl: data.TTL,
				expiresAt: data.ExpiresAt
			};
			resolving = false;
		} catch (err) {
			errorResolve = err instanceof Error ? err.message : 'Failed to resolve MemNS name';
			resolving = false;
		}
	}

	onMount(() => {
		fetchKeys();
	});
</script>

<div class="flex flex-col gap-6">
	<!-- Page Header -->
	<div class="border-b border-slate-800 pb-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
		<div>
			<h1 class="text-2xl font-bold text-slate-50">MemNS Namespaces</h1>
			<p class="text-xs text-slate-400 font-mono mt-1">Manage mutable cryptographic pointers and keys</p>
		</div>
		<div class="flex bg-slate-900 border border-slate-800 p-0.5 rounded-lg text-xs font-mono">
			<button 
				onclick={() => activeTab = 'keys'} 
				class={`px-4 py-1.5 rounded-md transition-all ${activeTab === 'keys' ? 'bg-cyan-500 text-slate-950 font-bold' : 'text-slate-400 hover:text-slate-200'}`}
			>
				Keyring & Publish
			</button>
			<button 
				onclick={() => activeTab = 'resolve'} 
				class={`px-4 py-1.5 rounded-md transition-all ${activeTab === 'resolve' ? 'bg-cyan-500 text-slate-950 font-bold' : 'text-slate-400 hover:text-slate-200'}`}
			>
				Name Resolver
			</button>
		</div>
	</div>

	{#if activeTab === 'keys'}
		<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
			<!-- Keyring Management Panel -->
			<div class="lg:col-span-2 space-y-6">
				<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4">
					<div class="flex items-center justify-between border-b border-slate-800 pb-3">
						<h2 class="font-bold text-sm text-slate-200 font-mono flex items-center gap-2">
							<Icon icon="ph:key" class="w-4 h-4 text-cyan-400" />
							Active Cryptographic Keyring
						</h2>
						<button 
							onclick={fetchKeys} 
							class="text-slate-500 hover:text-slate-300 transition-colors"
							title="Reload Keyring"
						>
							<Icon icon="ph:arrows-clockwise" class="w-4 h-4" />
						</button>
					</div>

					{#if loadingKeys && keys.length === 0}
						<div class="py-12 flex justify-center items-center">
							<span class="text-xs text-slate-500 font-mono">Loading keys...</span>
						</div>
					{:else if errorKeys}
						<div class="bg-red-950/20 border border-red-900/40 text-red-400 p-4 rounded-xl text-xs font-mono">
							{errorKeys}
						</div>
					{:else if keys.length === 0}
						<div class="py-12 border-2 border-dashed border-slate-800 rounded-xl flex flex-col items-center justify-center gap-2 text-center px-4">
							<span class="text-2xl text-slate-600"><Icon icon="ph:key-hole" /></span>
							<h3 class="text-xs font-bold text-slate-400">No cryptographic keys found</h3>
							<p class="text-[11px] text-slate-500 max-w-xs">Generate a key on the right panel to publish mutable records to MemNS</p>
						</div>
					{:else}
						<div class="divide-y divide-slate-800">
							{#each keys as key}
								<div class="py-4 first:pt-0 last:pb-0 flex flex-col sm:flex-row sm:items-center justify-between gap-4 font-mono text-xs">
									<div class="space-y-1.5 max-w-full overflow-hidden">
										<div class="flex items-center gap-2">
											<span class="font-bold text-slate-200 text-sm">{key.name}</span>
											<span class="px-1.5 py-0.5 rounded bg-slate-800 border border-slate-750 text-[10px] text-slate-400 uppercase">
												{key.type}
											</span>
										</div>
										<div class="flex flex-col gap-0.5 text-slate-500">
											<span class="text-[9px] uppercase tracking-wider text-slate-600">MemNS Target Name:</span>
											<a href={`${base}/memns/${key.memns_name.replace('/memns/', '')}`} class="text-cyan-400/90 hover:underline break-all hover:text-cyan-400 font-semibold">
												{key.memns_name}
											</a>
										</div>
										<div class="flex flex-col gap-0.5 text-slate-500">
											<span class="text-[9px] uppercase tracking-wider text-slate-600">Public Key Hash:</span>
											<span class="break-all font-mono text-[10px]">{key.pubkey}</span>
										</div>
									</div>
									<div class="flex items-center gap-2 font-sans">
										<button 
											onclick={() => { selectedKey = key.name; successPublish = null; errorPublish = null; }}
											class="px-3 py-1.5 rounded-lg bg-slate-850 hover:bg-slate-800 text-cyan-400 border border-slate-750 text-xs font-bold transition-all"
										>
											Select to Publish
										</button>
										<button 
											onclick={() => handleDeleteKey(key.name)}
											class="p-2 rounded-lg bg-red-950/20 hover:bg-red-950/40 text-red-400 border border-red-900/30 transition-colors"
											title="Delete Key"
										>
											<Icon icon="ph:trash" class="w-4 h-4" />
										</button>
									</div>
								</div>
							{/each}
						</div>
					{/if}
				</div>
			</div>

			<!-- Generation & Publishing Forms -->
			<div class="space-y-6">
				<!-- Generate Key Card -->
				<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4">
					<h2 class="font-bold text-sm text-slate-200 font-mono border-b border-slate-800 pb-3 flex items-center gap-2">
						<Icon icon="ph:plus-circle" class="w-4 h-4 text-cyan-400" />
						Generate New Key
					</h2>
					<form onsubmit={handleGenerateKey} class="flex flex-col gap-4 text-xs font-mono">
						<div class="flex flex-col gap-1.5">
							<label class="text-slate-400" for="key-name">Key Name / Alias</label>
							<input 
								id="key-name"
								type="text" 
								bind:value={newKeyName}
								placeholder="e.g. blog-key"
								required
								class="w-full bg-slate-950 border border-slate-800 text-slate-200 placeholder-slate-600 px-3.5 py-2 rounded-lg focus:outline-none focus:border-slate-500"
							/>
						</div>
						<div class="flex flex-col gap-1.5">
							<label class="text-slate-400" for="key-type">Algorithm</label>
							<select 
								id="key-type"
								bind:value={newKeyType}
								class="w-full bg-slate-950 border border-slate-800 text-slate-200 px-3 py-2 rounded-lg focus:outline-none focus:border-slate-500"
							>
								<option value="ed25519">Ed25519 (Recommended)</option>
								<option value="secp256k1">Secp256k1</option>
							</select>
						</div>
						
						{#if errorGen}
							<div class="bg-red-950/20 border border-red-900/40 text-red-400 p-2.5 rounded-lg text-[11px]">
								{errorGen}
							</div>
						{/if}

						<button 
							type="submit" 
							disabled={generatingKey}
							class="w-full px-4 py-2 rounded-lg bg-cyan-500 hover:bg-cyan-600 text-slate-950 font-bold text-xs transition-colors flex items-center justify-center gap-1.5 disabled:opacity-50 font-sans"
						>
							{#if generatingKey}
								<Icon icon="ph:spinner-gap" class="animate-spin w-4 h-4" />
								Generating...
							{:else}
								<Icon icon="ph:lightning" class="w-4 h-4" />
								Generate Key
							{/if}
						</button>
					</form>
				</div>

				<!-- Publish Record Card -->
				<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4">
					<h2 class="font-bold text-sm text-slate-200 font-mono border-b border-slate-800 pb-3 flex items-center gap-2">
						<Icon icon="ph:paper-plane-tilt" class="w-4 h-4 text-cyan-400" />
						Publish / Update Record
					</h2>
					<form onsubmit={handlePublish} class="flex flex-col gap-4 text-xs font-mono">
						<div class="flex flex-col gap-1.5">
							<label class="text-slate-400" for="publish-key">Select Key</label>
							<select 
								id="publish-key"
								bind:value={selectedKey}
								required
								class="w-full bg-slate-950 border border-slate-800 text-slate-200 px-3 py-2 rounded-lg focus:outline-none focus:border-slate-500"
							>
								<option value="" disabled>-- Choose a key --</option>
								{#each keys as key}
									<option value={key.name}>{key.name} ({key.memns_name.slice(0, 12)}...)</option>
								{/each}
							</select>
						</div>
						<div class="flex flex-col gap-1.5">
							<label class="text-slate-400" for="publish-value">Target Value (MID or Namespace)</label>
							<input 
								id="publish-value"
								type="text" 
								bind:value={publishValue}
								placeholder="e.g. /mem/membafzbe..."
								required
								class="w-full bg-slate-950 border border-slate-800 text-slate-200 placeholder-slate-600 px-3.5 py-2 rounded-lg focus:outline-none focus:border-slate-500"
							/>
						</div>
						<div class="flex flex-col gap-1.5">
							<label class="text-slate-400" for="publish-ttl">TTL (seconds)</label>
							<input 
								id="publish-ttl"
								type="number" 
								bind:value={publishTTL}
								required
								class="w-full bg-slate-950 border border-slate-800 text-slate-200 px-3.5 py-2 rounded-lg focus:outline-none focus:border-slate-500"
							/>
						</div>
						<div class="flex flex-col gap-1.5">
							<label class="text-slate-400" for="publish-msg">Publish Message (Optional)</label>
							<input 
								id="publish-msg"
								type="text" 
								bind:value={publishMsg}
								placeholder="e.g. Initial website deployment"
								class="w-full bg-slate-950 border border-slate-800 text-slate-200 placeholder-slate-600 px-3.5 py-2 rounded-lg focus:outline-none focus:border-slate-500"
							/>
						</div>

						{#if successPublish}
							<div class="bg-emerald-950/20 border border-emerald-900/40 text-emerald-400 p-2.5 rounded-lg text-[11px]">
								{successPublish}
							</div>
						{/if}
						
						{#if errorPublish}
							<div class="bg-red-950/20 border border-red-900/40 text-red-400 p-2.5 rounded-lg text-[11px]">
								{errorPublish}
							</div>
						{/if}

						<button 
							type="submit" 
							disabled={publishing}
							class="w-full px-4 py-2 rounded-lg bg-cyan-500 hover:bg-cyan-600 text-slate-950 font-bold text-xs transition-colors flex items-center justify-center gap-1.5 disabled:opacity-50 font-sans"
						>
							{#if publishing}
								<Icon icon="ph:spinner-gap" class="animate-spin w-4 h-4" />
								Publishing...
							{:else}
								<Icon icon="ph:globe" class="w-4 h-4" />
								Publish MemNS Record
							{/if}
						</button>
					</form>
				</div>
			</div>
		</div>
	{:else}
		<!-- Name Resolver Tab -->
		<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
			<div class="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4">
				<h2 class="font-bold text-sm text-slate-200 font-mono border-b border-slate-800 pb-3 flex items-center gap-2">
					<Icon icon="ph:magnifying-glass" class="w-4 h-4 text-cyan-400" />
					Resolve Mutable Name
				</h2>
				<form onsubmit={handleResolve} class="flex gap-2 font-mono text-xs">
					<input 
						type="text" 
						bind:value={resolveInput}
						placeholder="Enter MemNS target name, e.g. k51qziw..."
						required
						class="flex-grow bg-slate-950 border border-slate-800 text-slate-200 placeholder-slate-650 px-3.5 py-2.5 rounded-lg focus:outline-none focus:border-slate-500"
					/>
					<button 
						type="submit" 
						disabled={resolving}
						class="px-6 py-2.5 rounded-lg bg-cyan-500 hover:bg-cyan-600 text-slate-950 font-bold text-xs transition-colors flex items-center gap-1.5 disabled:opacity-50 font-sans"
					>
						{#if resolving}
							<Icon icon="ph:spinner-gap" class="animate-spin w-4 h-4" />
							Resolving...
						{:else}
							Resolve
						{/if}
					</button>
				</form>

				{#if errorResolve}
					<div class="bg-red-950/20 border border-red-900/40 text-red-400 p-4 rounded-xl text-xs font-mono mt-2">
						{errorResolve}
					</div>
				{/if}

				{#if resolvedResult}
					<div class="mt-4 bg-slate-950/40 border border-slate-750 p-6 rounded-xl flex flex-col gap-4">
						<h3 class="font-bold text-slate-200 border-b border-slate-800 pb-2 text-xs font-mono">
							Resolved Target Record
						</h3>
						<dl class="grid grid-cols-1 sm:grid-cols-2 gap-4 font-mono text-xs">
							<div class="flex flex-col gap-1 sm:col-span-2 bg-slate-900 border border-slate-800 p-3 rounded-lg">
								<span class="text-slate-500 uppercase text-[9px]">Resolved Value</span>
								<a href={`${base}/mid/${resolvedResult.value.replace('/mem/', '')}`} class="text-cyan-400 hover:underline text-sm font-bold break-all">
									{resolvedResult.value}
								</a>
							</div>
							<div class="flex flex-col gap-1">
								<span class="text-slate-500 uppercase text-[9px]">Sequence Number</span>
								<span class="text-slate-200 font-bold text-sm">{resolvedResult.sequence}</span>
							</div>
							<div class="flex flex-col gap-1">
								<span class="text-slate-500 uppercase text-[9px]">Expires At</span>
								<span class="text-slate-200 font-bold text-sm">{new Date(resolvedResult.expiresAt).toISOString().replace('T', ' ').slice(0, 19)} UTC</span>
							</div>
						</dl>
					</div>
				{/if}
			</div>

			<!-- Info Helper Card -->
			<div class="bg-slate-900 border border-slate-800 rounded-xl p-6 flex flex-col gap-4">
				<h2 class="font-bold text-sm text-slate-200 font-mono border-b border-slate-800 pb-3 flex items-center gap-2">
					<Icon icon="ph:info" class="w-4 h-4 text-cyan-400" />
					What is MemNS?
				</h2>
				<div class="text-[11px] font-mono text-slate-400 space-y-3 leading-relaxed">
					<p>
						<strong class="text-cyan-400 font-sans">MemNS (Membuss Name System)</strong> is a decentralized, cryptographically signed lookup registry similar to IPFS's IPNS.
					</p>
					<p>
						Because CIDs (MIDs) change every time files are updated, MemNS creates stable, static pointer names (e.g. <code>k51qziw...</code>) mapping to the latest CID.
					</p>
					<p>
						Keys are generated using standard asymmetric cryptography (Ed25519 or Secp256k1) and stored securely in your local keyring.
					</p>
					<p>
						When you publish a record, the local node signs it using the chosen private key and propagates it over the Mem-DHT network.
					</p>
				</div>
			</div>
		</div>
	{/if}
</div>
