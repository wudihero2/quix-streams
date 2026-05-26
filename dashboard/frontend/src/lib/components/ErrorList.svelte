<script lang="ts">
	import type { ErrorEntry } from '$lib/types';

	let { errors = [], limit = 0 }: { errors: ErrorEntry[]; limit?: number } = $props();

	let displayErrors = $derived(limit > 0 ? errors.slice(-limit) : errors);
</script>

{#if displayErrors.length === 0}
	<p class="text-sm text-gray-500">No errors recorded</p>
{:else}
	<div class="space-y-2">
		{#each displayErrors as error}
			<div class="rounded border border-gray-800 p-3">
				<div class="flex items-start justify-between">
					<div class="flex items-center gap-2">
						<span class="rounded bg-red-900/50 px-2 py-0.5 text-xs text-red-300">
							{error.key}
						</span>
						<span class="text-xs text-gray-500">x{error.count}</span>
					</div>
					{#if error.last_seen}
						<span class="text-xs text-gray-600">
							{new Date(error.last_seen * 1000).toLocaleTimeString()}
						</span>
					{/if}
				</div>
				{#if error.samples.length > 0}
					<p class="mt-1 truncate text-xs text-gray-400">{error.samples[0].message}</p>
				{/if}
			</div>
		{/each}
	</div>
{/if}
