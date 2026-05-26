from quix_metrics.errors import ErrorInterceptor


def test_error_interceptor_collects():
    interceptor = ErrorInterceptor()
    original_called = []

    def original_callback(exc, row, logger_inst):
        original_called.append(exc)
        return False

    wrapped = interceptor.wrap_processing_error(original_callback)

    class FakeRow:
        topic = "test-topic"
        partition = 0
        offset = 42

    exc = ValueError("test error")
    result = wrapped(exc, FakeRow(), None)
    assert result is False
    assert len(original_called) == 1

    errors = interceptor.collect_and_reset()
    assert len(errors) == 1
    assert errors[0]["key"] == "processing:ValueError"
    assert errors[0]["count"] == 1
    assert len(errors[0]["samples"]) == 1
    assert errors[0]["samples"][0]["message"] == "test error"


def test_error_interceptor_max_samples():
    interceptor = ErrorInterceptor()

    def noop(exc, row, logger_inst):
        return True

    wrapped = interceptor.wrap_processing_error(noop)

    class FakeRow:
        topic = "t"
        partition = 0
        offset = 0

    for i in range(10):
        wrapped(ValueError("err"), FakeRow(), None)

    errors = interceptor.collect_and_reset()
    assert errors[0]["count"] == 10
    assert len(errors[0]["samples"]) == 3  # MAX_SAMPLES


def test_error_interceptor_reset():
    interceptor = ErrorInterceptor()

    def noop(exc, row, logger_inst):
        return True

    wrapped = interceptor.wrap_processing_error(noop)

    class FakeRow:
        topic = "t"
        partition = 0
        offset = 0

    wrapped(ValueError("err"), FakeRow(), None)
    interceptor.collect_and_reset()

    errors = interceptor.collect_and_reset()
    assert errors == []
