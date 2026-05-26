<script lang="ts">
	import type { ResourceSnapshot } from '$lib/types';

	let { data }: { data: ResourceSnapshot } = $props();

	let memoryMB = $derived((data.memory_rss_bytes / (1024 * 1024)).toFixed(1));
</script>

<div class="space-y-4">
	<!-- CPU -->
	<div>
		<div class="mb-1 flex justify-between text-sm">
			<span class="text-gray-400">CPU</span>
			<span>{data.cpu_percent.toFixed(1)}%</span>
		</div>
		<div class="h-2 rounded-full bg-gray-800">
			<div
				class="h-2 rounded-full transition-all {data.cpu_percent > 80 ? 'bg-red-500' : data.cpu_percent > 50 ? 'bg-yellow-500' : 'bg-green-500'}"
				style="width: {Math.min(data.cpu_percent, 100)}%"
			></div>
		</div>
	</div>

	<!-- Memory -->
	<div>
		<div class="mb-1 flex justify-between text-sm">
			<span class="text-gray-400">Memory (RSS)</span>
			<span>{memoryMB} MB</span>
		</div>
		<div class="h-2 rounded-full bg-gray-800">
			<div
				class="h-2 rounded-full bg-blue-500 transition-all"
				style="width: {Math.min(data.memory_rss_bytes / (512 * 1024 * 1024) * 100, 100)}%"
			></div>
		</div>
	</div>

	<!-- State Dir -->
	{#if data.state_dir_bytes != null}
		<div>
			<div class="mb-1 flex justify-between text-sm">
				<span class="text-gray-400">State Dir</span>
				<span>{(data.state_dir_bytes / (1024 * 1024)).toFixed(1)} MB</span>
			</div>
		</div>
	{/if}

	<!-- Disk -->
	{#if data.state_disk_total_bytes != null && data.state_disk_used_bytes != null}
		{@const pct = (data.state_disk_used_bytes / data.state_disk_total_bytes) * 100}
		<div>
			<div class="mb-1 flex justify-between text-sm">
				<span class="text-gray-400">Disk</span>
				<span>{pct.toFixed(1)}%</span>
			</div>
			<div class="h-2 rounded-full bg-gray-800">
				<div
					class="h-2 rounded-full transition-all {pct > 90 ? 'bg-red-500' : pct > 70 ? 'bg-yellow-500' : 'bg-purple-500'}"
					style="width: {pct}%"
				></div>
			</div>
		</div>
	{/if}
</div>
