<script lang="ts">
	import { metrics } from '$lib/stores/metrics.svelte';

	let filter = $state('');

	let filteredErrors = $derived(
		filter
			? metrics.errors.filter(
					(e) =>
						e.key.toLowerCase().includes(filter.toLowerCase()) ||
						e.samples.some((s) => s.message.toLowerCase().includes(filter.toLowerCase()))
				)
			: metrics.errors
	);
</script>

<div class="space-y-4">
	<div class="flex items-center justify-between">
		<h1 class="text-2xl font-bold">Errors</h1>
		<span class="text-sm text-gray-400">{metrics.errors.length} total</span>
	</div>

	<input
		type="text"
		bind:value={filter}
		placeholder="Filter errors..."
		class="w-full rounded-lg border border-gray-700 bg-gray-900 px-4 py-2 text-sm text-gray-100 placeholder-gray-500 focus:border-blue-500 focus:outline-none"
	/>

	{#if filteredErrors.length === 0}
		<div class="flex h-48 items-center justify-center rounded-lg bg-gray-900">
			<p class="text-gray-500">{filter ? 'No matching errors' : 'No errors recorded'}</p>
		</div>
	{:else}
		<div class="space-y-3">
			{#each filteredErrors as error (error.key)}
				<div class="rounded-lg border border-gray-800 bg-gray-900 p-4">
					<div class="flex items-start justify-between">
						<div class="flex items-center gap-3">
							<span class="rounded bg-red-900/50 px-2 py-1 text-xs font-medium text-red-300">
								{error.key}
							</span>
							<span class="text-sm font-medium text-gray-300">x{error.count}</span>
						</div>
						<div class="text-xs text-gray-500">
							{#if error.first_seen}
								{new Date(error.first_seen * 1000).toLocaleString()}
							{/if}
						</div>
					</div>

					{#each error.samples as sample, i (i)}
						<div class="mt-3 rounded border border-gray-800 bg-gray-950 p-3">
							<div class="flex items-center justify-between">
								<span class="text-xs font-medium text-gray-400">Sample {i + 1}</span>
								<span class="text-xs text-gray-600">
									{new Date(sample.timestamp * 1000).toLocaleTimeString()}
								</span>
							</div>
							<p class="mt-1 text-sm text-red-300">{sample.message}</p>
							{#if sample.context && Object.keys(sample.context).length > 0}
								<div class="mt-2 text-xs text-gray-500">
									{#each Object.entries(sample.context) as [key, value] (key)}
										{#if value != null}
											<span class="mr-3">{key}: {value}</span>
										{/if}
									{/each}
								</div>
							{/if}
							{#if sample.traceback.length > 0}
								<details class="mt-2">
									<summary class="cursor-pointer text-xs text-gray-500 hover:text-gray-400">Traceback</summary>
									<pre class="mt-1 overflow-x-auto text-xs text-gray-500">{sample.traceback.join('')}</pre>
								</details>
							{/if}
						</div>
					{/each}
				</div>
			{/each}
		</div>
	{/if}
</div>
