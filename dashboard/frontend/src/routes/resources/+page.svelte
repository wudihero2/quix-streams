<script lang="ts">
	import { metrics } from '$lib/stores/metrics.svelte';
	import { onMount } from 'svelte';
	import { Chart, registerables } from 'chart.js';

	let cpuCanvas: HTMLCanvasElement = $state()! as HTMLCanvasElement;
	let memCanvas: HTMLCanvasElement = $state()! as HTMLCanvasElement;
	let cpuChart: Chart | null = null;
	let memChart: Chart | null = null;

	onMount(() => {
		Chart.register(...registerables);

		const chartOptions = {
			responsive: true,
			maintainAspectRatio: false,
			plugins: { legend: { labels: { color: '#9ca3af' } } },
			scales: {
				x: { ticks: { color: '#6b7280' }, grid: { color: '#1f2937' } },
				y: { ticks: { color: '#6b7280' }, grid: { color: '#1f2937' } },
			},
		};

		cpuChart = new Chart(cpuCanvas, {
			type: 'line',
			data: {
				labels: [],
				datasets: [
					{
						label: 'CPU %',
						data: [],
						borderColor: '#f59e0b',
						backgroundColor: 'rgba(245, 158, 11, 0.1)',
						fill: true,
						tension: 0.3,
						pointRadius: 0,
					},
				],
			},
			options: { ...chartOptions, scales: { ...chartOptions.scales, y: { ...chartOptions.scales.y, min: 0, max: 100 } } },
		});

		memChart = new Chart(memCanvas, {
			type: 'line',
			data: {
				labels: [],
				datasets: [
					{
						label: 'RSS (MB)',
						data: [],
						borderColor: '#3b82f6',
						backgroundColor: 'rgba(59, 130, 246, 0.1)',
						fill: true,
						tension: 0.3,
						pointRadius: 0,
					},
					{
						label: 'VMS (MB)',
						data: [],
						borderColor: '#8b5cf6',
						backgroundColor: 'rgba(139, 92, 246, 0.1)',
						fill: true,
						tension: 0.3,
						pointRadius: 0,
					},
				],
			},
			options: chartOptions,
		});

		return () => {
			cpuChart?.destroy();
			memChart?.destroy();
		};
	});

	$effect(() => {
		const data = metrics.resources;
		if (!cpuChart || !memChart || data.length === 0) return;

		const labels = data.map((d) => new Date(d.timestamp * 1000).toLocaleTimeString());

		cpuChart.data.labels = labels;
		cpuChart.data.datasets[0].data = data.map((d) => d.cpu_percent);
		cpuChart.update('none');

		memChart.data.labels = labels;
		memChart.data.datasets[0].data = data.map((d) => d.memory_rss_bytes / (1024 * 1024));
		memChart.data.datasets[1].data = data.map((d) => d.memory_vms_bytes / (1024 * 1024));
		memChart.update('none');
	});

	let latestResources = $derived(
		metrics.resources.length > 0
			? metrics.resources[metrics.resources.length - 1]
			: null
	);
</script>

<div class="space-y-6">
	<h1 class="text-2xl font-bold">Resource Monitoring</h1>

	<!-- Current values -->
	{#if latestResources}
		<div class="grid grid-cols-1 gap-4 md:grid-cols-4">
			<div class="rounded-lg bg-gray-900 p-4">
				<div class="text-sm text-gray-400">CPU</div>
				<div class="text-2xl font-bold">{latestResources.cpu_percent.toFixed(1)}%</div>
			</div>
			<div class="rounded-lg bg-gray-900 p-4">
				<div class="text-sm text-gray-400">RSS Memory</div>
				<div class="text-2xl font-bold">{(latestResources.memory_rss_bytes / (1024 * 1024)).toFixed(1)} MB</div>
			</div>
			<div class="rounded-lg bg-gray-900 p-4">
				<div class="text-sm text-gray-400">VMS Memory</div>
				<div class="text-2xl font-bold">{(latestResources.memory_vms_bytes / (1024 * 1024)).toFixed(1)} MB</div>
			</div>
			{#if latestResources.state_dir_bytes != null}
				<div class="rounded-lg bg-gray-900 p-4">
					<div class="text-sm text-gray-400">State Dir</div>
					<div class="text-2xl font-bold">{(latestResources.state_dir_bytes / (1024 * 1024)).toFixed(1)} MB</div>
				</div>
			{/if}
		</div>
	{/if}

	<!-- CPU chart -->
	<div class="rounded-lg bg-gray-900 p-4">
		<h2 class="mb-3 text-lg font-semibold">CPU Usage Over Time</h2>
		<div class="h-64">
			{#if metrics.resources.length === 0}
				<div class="flex h-full items-center justify-center text-sm text-gray-500">
					Waiting for resource data...
				</div>
			{:else}
				<canvas bind:this={cpuCanvas}></canvas>
			{/if}
		</div>
	</div>

	<!-- Memory chart -->
	<div class="rounded-lg bg-gray-900 p-4">
		<h2 class="mb-3 text-lg font-semibold">Memory Usage Over Time</h2>
		<div class="h-64">
			{#if metrics.resources.length === 0}
				<div class="flex h-full items-center justify-center text-sm text-gray-500">
					Waiting for resource data...
				</div>
			{:else}
				<canvas bind:this={memCanvas}></canvas>
			{/if}
		</div>
	</div>
</div>
