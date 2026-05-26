<script lang="ts">
	import type { ThroughputSnapshot } from '$lib/types';
	import { onMount } from 'svelte';
	import { Chart, registerables } from 'chart.js';

	let { data = [] }: { data: ThroughputSnapshot[] } = $props();

	let canvas: HTMLCanvasElement = $state()! as HTMLCanvasElement;
	let chart: Chart | null = null;

	onMount(() => {
		Chart.register(...registerables);

		chart = new Chart(canvas, {
			type: 'line',
			data: {
				labels: [],
				datasets: [
					{
						label: 'Messages',
						data: [],
						borderColor: '#3b82f6',
						backgroundColor: 'rgba(59, 130, 246, 0.1)',
						fill: true,
						tension: 0.3,
						pointRadius: 0,
					},
					{
						label: 'Bytes',
						data: [],
						borderColor: '#10b981',
						backgroundColor: 'rgba(16, 185, 129, 0.1)',
						fill: true,
						tension: 0.3,
						pointRadius: 0,
						yAxisID: 'y1',
					},
				],
			},
			options: {
				responsive: true,
				maintainAspectRatio: false,
				interaction: { mode: 'index', intersect: false },
				plugins: {
					legend: { labels: { color: '#9ca3af' } },
				},
				scales: {
					x: {
						ticks: { color: '#6b7280' },
						grid: { color: '#1f2937' },
					},
					y: {
						position: 'left',
						ticks: { color: '#6b7280' },
						grid: { color: '#1f2937' },
						title: { display: true, text: 'Messages', color: '#9ca3af' },
					},
					y1: {
						position: 'right',
						ticks: { color: '#6b7280' },
						grid: { drawOnChartArea: false },
						title: { display: true, text: 'Bytes', color: '#9ca3af' },
					},
				},
			},
		});

		return () => chart?.destroy();
	});

	$effect(() => {
		if (!chart) return;
		const labels = data.map((d) => {
			const date = new Date(d.timestamp * 1000);
			return date.toLocaleTimeString();
		});
		chart.data.labels = labels;
		chart.data.datasets[0].data = data.map((d) => d.total_messages);
		chart.data.datasets[1].data = data.map((d) => d.total_bytes);
		chart.update('none');
	});
</script>

<div class="relative h-64">
	<canvas bind:this={canvas}></canvas>
	{#if data.length === 0}
		<div class="absolute inset-0 flex items-center justify-center text-sm text-gray-500">
			Waiting for throughput data...
		</div>
	{/if}
</div>
