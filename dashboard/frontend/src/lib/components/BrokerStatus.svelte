<script lang="ts">
	import type { BrokerHealthSnapshot } from '$lib/types';

	let { data = null }: { data: BrokerHealthSnapshot | null } = $props();
</script>

{#if data === null}
	<p class="text-sm text-gray-500">No broker data</p>
{:else}
	<div class="space-y-2">
		<div class="flex items-center gap-2">
			<span class="h-3 w-3 rounded-full {data.all_brokers_up ? 'bg-green-500' : 'bg-red-500'}"></span>
			<span class="text-sm">{data.all_brokers_up ? 'All brokers healthy' : 'Broker issues detected'}</span>
		</div>
		<div class="space-y-1">
			{#each Object.entries(data.brokers) as [name, broker]}
				<div class="flex items-center gap-2 text-sm">
					<span class="h-2 w-2 rounded-full {broker.is_up ? 'bg-green-500' : 'bg-red-500'}"></span>
					<span class="font-mono text-xs text-gray-400">{name}</span>
					<span class="text-xs text-gray-500">{broker.state}</span>
				</div>
			{/each}
		</div>
		{#if data.any_broker_unavailable_since}
			<p class="text-xs text-red-400">
				Unavailable since {new Date(data.any_broker_unavailable_since * 1000).toLocaleTimeString()}
			</p>
		{/if}
	</div>
{/if}
