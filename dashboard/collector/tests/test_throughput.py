from quix_metrics.throughput import ThroughputTracker


def test_throughput_tracker_counts():
    tracker = ThroughputTracker()
    tracker.on_message_processed("topic-a", 0, 1)
    tracker.on_message_processed("topic-a", 0, 2)
    tracker.on_message_processed("topic-a", 1, 1)

    data = tracker.collect_and_reset()
    assert data["total_messages"] == 3
    assert data["partitions"]["topic-a:0"]["message_count"] == 2
    assert data["partitions"]["topic-a:1"]["message_count"] == 1


def test_throughput_tracker_with_size():
    tracker = ThroughputTracker()
    tracker.on_message_processed_with_size("topic-a", 0, 1, 100)
    tracker.on_message_processed_with_size("topic-a", 0, 2, 200)

    data = tracker.collect_and_reset()
    assert data["total_messages"] == 2
    assert data["total_bytes"] == 300
    assert data["partitions"]["topic-a:0"]["byte_count"] == 300


def test_throughput_tracker_reset():
    tracker = ThroughputTracker()
    tracker.on_message_processed("topic-a", 0, 1)
    tracker.collect_and_reset()

    data = tracker.collect_and_reset()
    assert data["total_messages"] == 0
    assert data["partitions"] == {}
