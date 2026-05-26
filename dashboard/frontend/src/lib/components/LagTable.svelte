<script lang="ts">
	import type { PartitionThroughput } from '$lib/types';

	let { partitions = {} }: { partitions: Record<string, PartitionThroughput> } = $props();

	let entries = $derived(Object.entries(partitions).sort((a, b) => a[0].localeCompare(b[0])));
</script>

{#if entries.length === 0}
	<p class="text-sm text-gray-500">No partition data</p>
{:else}
	<div class="overflow-x-auto">
		<table class="w-full text-left text-sm">
			<thead class="text-xs uppercase text-gray-400">
				<tr>
					<th class="px-3 py-2">Topic:Partition</th>
					<th class="px-3 py-2 text-right">Messages</th>
					<th class="px-3 py-2 text-right">Bytes</th>
				</tr>
			</thead>
			<tbody>
				{#each entries as [key, partition]}
					<tr class="border-t border-gray-800">
						<td class="px-3 py-2 font-mono text-xs">{key}</td>
						<td class="px-3 py-2 text-right">{partition.message_count}</td>
						<td class="px-3 py-2 text-right">{formatBytes(partition.byte_count)}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}

<script lang="ts" module>
	function formatBytes(bytes: number): string {
		if (bytes === 0) return '0 B';
		const k = 1024;
		const sizes = ['B', 'KB', 'MB', 'GB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
	}
</script>
