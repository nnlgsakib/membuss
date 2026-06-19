<script lang="ts">
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { base } from '$app/paths';

	onMount(() => {
		goto(`${base}/mid/${page.params.mid}`);
	});
</script>

	<div class="flex items-center justify-center py-20 text-slate-500 font-mono text-xs">
	Redirecting to content detail dashboard...
</div>
