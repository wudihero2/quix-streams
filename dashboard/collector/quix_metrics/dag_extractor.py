"""Extract DAG topology from a DataFrameRegistry."""

from typing import Any


def extract_dag(registry) -> dict[str, Any]:
    """
    Build a DAG JSON structure from the DataFrameRegistry.

    Returns a dict with:
      - nodes: list of {id, label, type}
      - edges: list of {source, target}
      - topics: list of topic names
    """
    nodes = []
    edges = []
    topics = []
    seen_nodes = set()
    seen_edges = set()

    for topic_name, root_stream in registry._registry.items():
        topics.append(topic_name)

        # Add topic node
        topic_node_id = f"topic:{topic_name}"
        if topic_node_id not in seen_nodes:
            nodes.append({
                "id": topic_node_id,
                "label": topic_name,
                "type": "topic",
            })
            seen_nodes.add(topic_node_id)

        # Walk the stream tree
        try:
            tree = root_stream.full_tree()
        except Exception:
            tree = [root_stream]

        prev_node_id = topic_node_id
        for i, stream in enumerate(tree):
            func = stream.func
            func_type = func.__class__.__name__
            func_name = getattr(func.func, "__qualname__", str(func.func))

            node_id = f"stream:{topic_name}:{i}"
            if node_id not in seen_nodes:
                nodes.append({
                    "id": node_id,
                    "label": f"{func_type}: {func_name}",
                    "type": func_type.lower(),
                })
                seen_nodes.add(node_id)

            edge_key = (prev_node_id, node_id)
            if edge_key not in seen_edges:
                edges.append({"source": prev_node_id, "target": node_id})
                seen_edges.add(edge_key)
            prev_node_id = node_id

    return {"nodes": nodes, "edges": edges, "topics": topics}
