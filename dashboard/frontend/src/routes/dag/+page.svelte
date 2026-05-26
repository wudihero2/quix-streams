<script lang="ts">
	import { metrics } from '$lib/stores/metrics.svelte';
	import DagView from '$lib/components/DagView.svelte';
</script>

<div class="space-y-4">
	<div class="flex items-center justify-between">
		<h1 class="text-2xl font-bold">Pipeline DAG</h1>
		{#if metrics.dag}
			<span class="text-sm text-gray-400">
				{metrics.dag.nodes.length} nodes &middot; {metrics.dag.edges.length} edges
			</span>
		{/if}
	</div>

	{#if metrics.dag && metrics.dag.nodes.length > 0}
		<div class="rounded-lg bg-gray-900" style="height: calc(100vh - 200px);">
			<DagView dag={metrics.dag} />
		</div>
	{:else}
		<div class="flex h-96 items-center justify-center rounded-lg bg-gray-900">
			<p class="text-gray-500">No DAG data available. Start a pipeline with MetricsAgent attached.</p>
		</div>
	{/if}
</div>
