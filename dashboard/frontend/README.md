# quix-dashboard-frontend

SvelteKit frontend for the Quix Streams monitoring dashboard. Connects to the backend via Server-Sent Events (SSE) and renders live pipeline metrics.

## Prerequisites

- Node.js >= 18
- Backend running at `http://localhost:8000` (see `dashboard/backend/`)

## Install & Run

```bash
cd dashboard/frontend
npm install
npm run dev
```

Open http://localhost:5173 in your browser.

## Build for Production

```bash
npm run build
npm run preview   # preview the production build locally
```

## Pages

| Route        | Description                                                  |
|--------------|--------------------------------------------------------------|
| `/`          | Dashboard overview: key metrics, mini DAG, throughput chart, partition table, resource bars, broker status, recent errors |
| `/dag`       | Full-screen interactive DAG visualization with pan/zoom      |
| `/errors`    | Filterable error list with counts, samples, and tracebacks   |
| `/resources` | CPU and memory charts over time with current values          |

## Tech Stack

| Library           | Purpose                           |
|-------------------|-----------------------------------|
| SvelteKit         | App framework                     |
| Svelte 5 (runes)  | Reactivity (`$state`, `$derived`, `$effect`, `$props`) |
| Tailwind CSS v4   | Styling                           |
| @xyflow/svelte    | DAG graph rendering               |
| dagre             | Automatic DAG layout              |
| chart.js          | Throughput and resource charts     |

## Architecture

```
Backend (localhost:8000)
    │
    │  SSE: GET /api/stream
    ▼
MetricsStore (metrics.svelte.ts)
    │
    │  Reactive $state fields:
    │   ├── dag
    │   ├── throughput
    │   ├── errors
    │   ├── resources
    │   └── broker_health
    │
    ▼
Pages & Components
    ├── +page.svelte          (Dashboard overview)
    ├── dag/+page.svelte      (Full-screen DAG)
    ├── errors/+page.svelte   (Error list)
    ├── resources/+page.svelte (Resource charts)
    │
    └── components/
         ├── DagView.svelte         (@xyflow/svelte + dagre)
         ├── ThroughputChart.svelte (chart.js line chart)
         ├── LagTable.svelte        (partition detail table)
         ├── ResourceGauges.svelte  (progress bars)
         ├── ErrorList.svelte       (error entries)
         └── BrokerStatus.svelte    (broker indicators)
```

## SSE Connection

The `MetricsStore` class (`src/lib/stores/metrics.svelte.ts`) manages the SSE connection:

- Connects on app mount, disconnects on unmount
- Auto-reconnects after 3 seconds on disconnect
- Connection status shown in the navbar (green/red dot)

The backend URL defaults to `http://localhost:8000`. To change it, edit the `API_BASE` constant in `src/lib/stores/metrics.svelte.ts`.

## Project Structure

```
src/
├── app.html                         # HTML shell
├── app.css                          # Tailwind import
├── routes/
│   ├── +layout.svelte               # Navbar, SSE connection lifecycle
│   ├── +page.svelte                 # Dashboard overview
│   ├── dag/+page.svelte             # DAG page
│   ├── errors/+page.svelte          # Errors page
│   └── resources/+page.svelte       # Resources page
└── lib/
    ├── types.ts                     # TypeScript interfaces
    ├── stores/
    │   └── metrics.svelte.ts        # SSE-backed reactive store
    └── components/
        ├── DagView.svelte
        ├── ThroughputChart.svelte
        ├── LagTable.svelte
        ├── ResourceGauges.svelte
        ├── ErrorList.svelte
        └── BrokerStatus.svelte
```
