<script lang="ts">
	import '../app.css';
	import { metrics } from '$lib/stores/metrics.svelte';
	import { onMount } from 'svelte';

	let { children } = $props();

	onMount(() => {
		metrics.connect();
		return () => metrics.disconnect();
	});
</script>

<div class="min-h-screen bg-gray-950 text-gray-100">
	<nav class="border-b border-gray-800 bg-gray-900">
		<div class="mx-auto flex max-w-7xl items-center gap-6 px-4 py-3">
			<a href="/" class="text-lg font-bold text-white">Quix Dashboard</a>
			<a href="/dag" class="text-sm text-gray-400 hover:text-white">DAG</a>
			<a href="/errors" class="text-sm text-gray-400 hover:text-white">Errors</a>
			<a href="/resources" class="text-sm text-gray-400 hover:text-white">Resources</a>
			<div class="ml-auto flex items-center gap-2">
				{#if metrics.status === 'connected'}
					<span class="h-2 w-2 rounded-full bg-green-500"></span>
					<span class="text-xs text-gray-500">Connected</span>
				{:else if metrics.status === 'connecting'}
					<span class="h-2 w-2 animate-pulse rounded-full bg-yellow-500"></span>
					<span class="text-xs text-gray-500">Connecting...</span>
				{:else}
					<span class="h-2 w-2 rounded-full bg-red-500"></span>
					<span class="text-xs text-gray-500">Disconnected</span>
				{/if}
			</div>
		</div>
	</nav>

	<main class="mx-auto max-w-7xl px-4 py-6">
		{@render children()}
	</main>
</div>
