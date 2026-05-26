<script lang="ts">
	import type { DagSnapshot } from '$lib/types';
	import {
		SvelteFlow,
		Background,
		Controls,
		type Node,
		type Edge,
	} from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';
	import dagre from 'dagre';

	let { dag }: { dag: DagSnapshot } = $props();

	let { layoutNodes, layoutEdges } = $derived.by(() => {
		const g = new dagre.graphlib.Graph();
		g.setDefaultEdgeLabel(() => ({}));
		g.setGraph({ rankdir: 'LR', ranksep: 80, nodesep: 40 });

		const nodes: Node[] = dag.nodes.map((n) => ({
			id: n.id,
			data: { label: n.label },
			position: { x: 0, y: 0 },
			style: getNodeStyle(n.type),
		}));

		const edges: Edge[] = dag.edges.map((e, i) => ({
			id: `edge-${i}`,
			source: e.source,
			target: e.target,
			animated: true,
			style: 'stroke: #6b7280;',
		}));

		nodes.forEach((node) => {
			g.setNode(node.id, { width: 180, height: 40 });
		});
		edges.forEach((edge) => {
			g.setEdge(edge.source, edge.target);
		});

		dagre.layout(g);

		const layoutNodes = nodes.map((node) => {
			const pos = g.node(node.id);
			return { ...node, position: { x: pos.x - 90, y: pos.y - 20 } };
		});

		return { layoutNodes, layoutEdges: edges };
	});
</script>

<div class="h-full w-full" style="min-height: 400px;">
	<SvelteFlow
		nodes={layoutNodes}
		edges={layoutEdges}
		fitView
		colorMode="dark"
	>
		<Background />
		<Controls />
	</SvelteFlow>
</div>

<script lang="ts" module>
	function getNodeStyle(type: string): string {
		switch (type) {
			case 'topic':
				return 'background: #1e3a5f; border: 1px solid #3b82f6; color: #93c5fd; border-radius: 8px; padding: 8px; font-size: 12px;';
			case 'applyfunction':
				return 'background: #1a3329; border: 1px solid #10b981; color: #6ee7b7; border-radius: 4px; padding: 8px; font-size: 12px;';
			case 'filterfunction':
				return 'background: #3b1f2b; border: 1px solid #f43f5e; color: #fda4af; border-radius: 4px; padding: 8px; font-size: 12px;';
			case 'updatefunction':
				return 'background: #2d2407; border: 1px solid #eab308; color: #fde047; border-radius: 4px; padding: 8px; font-size: 12px;';
			default:
				return 'background: #1f2937; border: 1px solid #4b5563; color: #d1d5db; border-radius: 4px; padding: 8px; font-size: 12px;';
		}
	}
</script>
