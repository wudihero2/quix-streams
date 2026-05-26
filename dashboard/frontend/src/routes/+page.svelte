<script lang="ts">
	import { metrics } from '$lib/stores/metrics.svelte';
	import ThroughputChart from '$lib/components/ThroughputChart.svelte';
	import LagTable from '$lib/components/LagTable.svelte';
	import ResourceGauges from '$lib/components/ResourceGauges.svelte';
	import ErrorList from '$lib/components/ErrorList.svelte';
	import BrokerStatus from '$lib/components/BrokerStatus.svelte';

	let latestThroughput = $derived(
		metrics.throughput.length > 0
			? metrics.throughput[metrics.throughput.length - 1]
			: null
	);

	let latestResources = $derived(
		metrics.resources.length > 0
			? metrics.resources[metrics.resources.length - 1]
			: null
	);
</script>

<div class="space-y-6">
	<h1 class="text-2xl font-bold">Pipeline Overview</h1>

	<!-- Top row: key metrics -->
	<div class="grid grid-cols-1 gap-4 md:grid-cols-4">
		<div class="rounded-lg bg-gray-900 p-4">
			<div class="text-sm text-gray-400">Messages/interval</div>
			<div class="text-2xl font-bold">{latestThroughput?.total_messages ?? 0}</div>
		</div>
		<div class="rounded-lg bg-gray-900 p-4">
			<div class="text-sm text-gray-400">Bytes/interval</div>
			<div class="text-2xl font-bold">{latestThroughput ? formatBytes(latestThroughput.total_bytes) : '0 B'}</div>
		</div>
		<div class="rounded-lg bg-gray-900 p-4">
			<div class="text-sm text-gray-400">CPU</div>
			<div class="text-2xl font-bold">{latestResources?.cpu_percent.toFixed(1) ?? '0'}%</div>
		</div>
		<div class="rounded-lg bg-gray-900 p-4">
			<div class="text-sm text-gray-400">Memory (RSS)</div>
			<div class="text-2xl font-bold">{latestResources ? formatBytes(latestResources.memory_rss_bytes) : '0 B'}</div>
		</div>
	</div>

	<!-- DAG mini view -->
	{#if metrics.dag}
		<div class="rounded-lg bg-gray-900 p-4">
			<div class="mb-2 flex items-center justify-between">
				<h2 class="text-lg font-semibold">Pipeline DAG</h2>
				<a href="/dag" class="text-sm text-blue-400 hover:text-blue-300">Full screen &rarr;</a>
			</div>
			<div class="text-sm text-gray-400">
				{metrics.dag.nodes.length} nodes, {metrics.dag.edges.length} edges, {metrics.dag.topics.length} topics
			</div>
		</div>
	{/if}

	<!-- Throughput chart -->
	<div class="rounded-lg bg-gray-900 p-4">
		<h2 class="mb-3 text-lg font-semibold">Throughput</h2>
		<ThroughputChart data={metrics.throughput} />
	</div>

	<!-- Lag table -->
	{#if latestThroughput}
		<div class="rounded-lg bg-gray-900 p-4">
			<h2 class="mb-3 text-lg font-semibold">Partition Details</h2>
			<LagTable partitions={latestThroughput.partitions} />
		</div>
	{/if}

	<!-- Bottom row: resources, brokers, errors -->
	<div class="grid grid-cols-1 gap-4 lg:grid-cols-3">
		<div class="rounded-lg bg-gray-900 p-4">
			<h2 class="mb-3 text-lg font-semibold">Resources</h2>
			{#if latestResources}
				<ResourceGauges data={latestResources} />
			{:else}
				<p class="text-sm text-gray-500">No data yet</p>
			{/if}
		</div>

		<div class="rounded-lg bg-gray-900 p-4">
			<h2 class="mb-3 text-lg font-semibold">Brokers</h2>
			<BrokerStatus data={metrics.broker_health} />
		</div>

		<div class="rounded-lg bg-gray-900 p-4">
			<div class="mb-3 flex items-center justify-between">
				<h2 class="text-lg font-semibold">Recent Errors</h2>
				<a href="/errors" class="text-sm text-blue-400 hover:text-blue-300">View all &rarr;</a>
			</div>
			<ErrorList errors={metrics.errors} limit={5} />
		</div>
	</div>
</div>

<script lang="ts" module>
	function formatBytes(bytes: number): string {
		if (bytes === 0) return '0 B';
		const k = 1024;
		const sizes = ['B', 'KB', 'MB', 'GB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
	}
</script>
