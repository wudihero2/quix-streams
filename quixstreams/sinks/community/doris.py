import logging
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from collections.abc import Callable, Mapping
from typing import Any, Literal
from urllib.parse import quote

try:
    import orjson
except ImportError as exc:
    raise ImportError(
        f"Package `{exc.name}` is missing: "
        'run "pip install quixstreams[doris]" to fix it'
    ) from exc

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError as exc:
    raise ImportError(
        f"Package `{exc.name}` is missing: "
        'run "pip install quixstreams[doris]" to fix it'
    ) from exc

from quixstreams.exceptions import QuixException
from quixstreams.models import HeadersTuples
from quixstreams.sinks import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBatch,
)
from quixstreams.sinks.base.item import SinkItem

__all__ = ("DorisSink", "DorisSinkException")

logger = logging.getLogger(__name__)

MergeType = Literal["APPEND", "DELETE", "MERGE"]
PartialUpdateMode = Literal["none", "fixed", "flexible"]
MetadataField = Literal["key", "topic", "partition", "offset", "headers", "timestamp"]
ALL_METADATA_FIELDS: set[MetadataField] = {
    "key", "topic", "partition", "offset", "headers", "timestamp",
}

_METADATA_COLUMN_MAP: dict[MetadataField, str] = {
    "key": "__key",
    "topic": "__topic",
    "partition": "__partition",
    "offset": "__offset",
    "headers": "__headers",
    "timestamp": "__timestamp",
}

TableName = Callable[[SinkItem], str] | str


class DorisSinkException(QuixException): ...


class DorisSink(BatchingSink):
    def __init__(
        self,
        host: str,
        http_port: int,
        username: str,
        password: str,
        database: str,
        table_name: TableName,
        timeout_seconds: int = 60,
        max_filter_ratio: float = 0.0,
        extra_headers: dict[str, str] | None = None,
        include_metadata: bool | set[MetadataField] = True,
        flatten_value: bool = True,
        partial_update: PartialUpdateMode = "none",
        partial_update_columns: list[str] | None = None,
        merge_type: MergeType = "APPEND",
        delete_condition: str | None = None,
        sequence_column: str | None = None,
        send_batch_parallelism: int | None = None,
        hidden_columns: list[str] | None = None,
        on_client_connect_success: ClientConnectSuccessCallback | None = None,
        on_client_connect_failure: ClientConnectFailureCallback | None = None,
    ):
        """
        A connector to sink topic data to Apache Doris via Stream Load.

        :param host: Doris FE host address.
        :param http_port: Doris FE HTTP port (default 8030).
        :param username: Doris username.
        :param password: Doris password.
        :param database: Target database name.
        :param table_name: Target table name as either a string or a callable
            which receives a SinkItem and returns a string.
        :param timeout_seconds: Stream Load timeout in seconds.
        :param max_filter_ratio: Max tolerable ratio of filtered (bad) rows,
            between 0.0 and 1.0. Default 0.0 means zero tolerance.
        :param extra_headers: Additional Stream Load headers
            (e.g. {"timezone": "Asia/Taipei", "columns": "col1,col2"}).
        :param include_metadata: Controls which Kafka metadata columns to include.
            True = all fields (__key, __topic, __partition, __offset, __headers, __timestamp).
            False = none.
            Set of field names = only those fields, e.g. {"key", "offset", "timestamp"}.
            Default True.
        :param flatten_value: If True (default), expand value dict fields as
            top-level columns. If False, store the entire value as a single
            JSON column named __value.
        :param partial_update: Partial column update mode for Unique Key tables.
            "none" (default) = full row insert/replace.
            "fixed" = all rows update the same columns (specified by
            partial_update_columns). Supports CSV/JSON.
            "flexible" = each row can update different columns (Doris 3.1+,
            JSON only). Best for CDC where each event may carry different fields.
        :param partial_update_columns: Required when partial_update="fixed".
            List of column names to update (must include all key columns).
            Ignored when partial_update is "none" or "flexible".
        :param merge_type: Data merge strategy for Unique Key tables.
            "APPEND" (default) = insert all rows.
            "DELETE" = delete rows matching imported keys.
            "MERGE" = rows matching delete_condition are deleted, rest appended.
        :param delete_condition: SQL WHERE expression for MERGE mode delete.
            Required when merge_type="MERGE". E.g. "is_deleted=1".
        :param sequence_column: Column name to control row replacement order
            in Unique Key tables. Row with larger sequence value wins.
            Maps to header function_column.sequence_col.
        :param send_batch_parallelism: Parallelism for batch data sending.
            Capped by BE config max_send_batch_parallelism_per_job.
        :param hidden_columns: List of Doris hidden columns present in data.
            E.g. ["__DORIS_DELETE_SIGN__", "__DORIS_SEQUENCE_COL__"].
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
        """
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._host = host
        self._http_port = http_port
        self._database = database
        self._table_name = _table_name_setter(table_name)
        self._auth = HTTPBasicAuth(username, password)
        self._timeout_seconds = timeout_seconds
        self._max_filter_ratio = max_filter_ratio
        self._extra_headers = extra_headers or {}
        self._flatten_value = flatten_value
        self._partial_update = partial_update
        if partial_update == "fixed" and not partial_update_columns:
            raise ValueError(
                "partial_update_columns is required when partial_update='fixed'"
            )
        if partial_update != "fixed" and partial_update_columns:
            logger.warning(
                "partial_update_columns is ignored when partial_update=%r",
                partial_update,
            )
        self._partial_update_columns = partial_update_columns
        self._merge_type = merge_type
        if merge_type == "MERGE" and not delete_condition:
            raise ValueError(
                "delete_condition is required when merge_type='MERGE'"
            )
        if merge_type != "MERGE" and delete_condition:
            raise ValueError(
                "delete_condition is only valid when merge_type='MERGE'"
            )
        if partial_update != "none" and merge_type != "APPEND":
            raise ValueError(
                f"partial_update='{partial_update}' cannot be used "
                f"with merge_type='{merge_type}'"
            )
        self._delete_condition = delete_condition
        self._sequence_column = sequence_column
        self._send_batch_parallelism = send_batch_parallelism
        self._hidden_columns = hidden_columns
        if include_metadata is True:
            self._metadata_fields = ALL_METADATA_FIELDS
        elif include_metadata is False:
            self._metadata_fields: set[MetadataField] = set()
        else:
            invalid = include_metadata - ALL_METADATA_FIELDS
            if invalid:
                raise ValueError(f"Unknown metadata fields: {invalid}")
            self._metadata_fields = include_metadata
        self._session: requests.Session | None = None

    def setup(self):
        self._session = requests.Session()
        self._session.should_strip_auth = lambda old_url, new_url: False
        self._session.auth = self._auth

        url = f"http://{self._host}:{self._http_port}/api/bootstrap"
        try:
            self._session.request("GET", url, timeout=10)
        except requests.ConnectionError as e:
            raise DorisSinkException(
                f"Cannot connect to Doris FE at "
                f"{self._host}:{self._http_port}: {e}"
            ) from e

    def cleanup(self):
        if self._session is not None:
            self._session.close()
            self._session = None

    def write(self, batch: SinkBatch):
        tables: dict[str, list[dict]] = {}
        for item in batch:
            table = self._table_name(item)
            rows = tables.setdefault(table, [])
            row = _item_to_row(
                item,
                topic=batch.topic,
                partition=batch.partition,
                metadata_fields=self._metadata_fields,
                flatten_value=self._flatten_value,
            )
            rows.append(row)

        for table, rows in tables.items():
            self._stream_load(table, rows)

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ):
        if self._flatten_value and not isinstance(value, Mapping):
            raise TypeError(
                f'Sink "{self.__class__.__name__}" with flatten_value=True '
                f"supports only dictionaries, got {type(value)}"
            )
        return super().add(
            value=value,
            key=key,
            timestamp=timestamp,
            headers=headers,
            topic=topic,
            partition=partition,
            offset=offset,
        )

    def _build_url(self, table: str) -> str:
        return (
            f"http://{self._host}:{self._http_port}"
            f"/api/{quote(self._database, safe='')}"
            f"/{quote(table, safe='')}/_stream_load"
        )

    def _stream_load(self, table: str, rows: list[dict]) -> None:
        if not rows:
            return

        body = b"\n".join(
            orjson.dumps(row, default=_orjson_default) for row in rows
        )

        headers = {
            "Expect": "100-continue",
            "format": "json",
            "read_json_by_line": "true",
            "strip_outer_array": "false",
            "label": f"quix_{_sanitize_label(table)}_{uuid.uuid4().hex}",
            "timeout": str(self._timeout_seconds),
            "max_filter_ratio": str(self._max_filter_ratio),
        }

        if self._partial_update == "fixed":
            headers["partial_columns"] = "true"
            headers["columns"] = ",".join(self._partial_update_columns)
        elif self._partial_update == "flexible":
            headers["unique_key_update_mode"] = "UPDATE_FLEXIBLE_COLUMNS"

        if self._merge_type != "APPEND":
            headers["merge_type"] = self._merge_type
        if self._delete_condition:
            headers["delete"] = self._delete_condition
        if self._sequence_column:
            headers["function_column.sequence_col"] = self._sequence_column
        if self._send_batch_parallelism is not None:
            headers["send_batch_parallelism"] = str(self._send_batch_parallelism)
        if self._hidden_columns:
            headers["hidden_columns"] = ",".join(self._hidden_columns)

        headers.update(self._extra_headers)

        url = self._build_url(table)
        try:
            resp = self._session.request(
                "PUT",
                url=url,
                data=body,
                headers=headers,
                timeout=self._timeout_seconds,
            )
        except requests.RequestException as e:
            raise DorisSinkException(
                f"Stream Load request failed for table '{table}': {e}"
            ) from e

        self._handle_response(resp, table, len(rows))

    def _handle_response(
        self, resp: requests.Response, table: str, expected_rows: int
    ) -> None:
        try:
            result = resp.json()
        except (ValueError, KeyError) as e:
            raise DorisSinkException(
                f"Invalid Stream Load response for table '{table}': "
                f"status={resp.status_code}, body={resp.text[:500]}"
            ) from e

        status = result.get("Status")
        if status == "Success":
            logger.info(
                f"Stream Load to '{table}': "
                f"loaded={result.get('NumberLoadedRows')}, "
                f"filtered={result.get('NumberFilteredRows', 0)}, "
                f"time={result.get('LoadTimeMs')}ms"
            )
            return

        if status == "Publish Timeout":
            logger.warning(
                f"Stream Load to '{table}' publish timeout — "
                f"data committed but visibility delayed: {result.get('Message')}"
            )
            return

        error_url = result.get("ErrorURL", "")
        raise DorisSinkException(
            f"Stream Load failed for table '{table}': "
            f"Status={status}, "
            f"Message={result.get('Message')}, "
            f"NumberTotalRows={result.get('NumberTotalRows')}, "
            f"NumberFilteredRows={result.get('NumberFilteredRows')}, "
            f"ErrorURL={error_url}"
        )


def _item_to_row(
    item: SinkItem,
    topic: str,
    partition: int,
    metadata_fields: set[MetadataField],
    flatten_value: bool,
) -> dict:
    if flatten_value:
        row = dict(item.value)
    else:
        row = {"__value": item.value}

    if "key" in metadata_fields:
        row[_METADATA_COLUMN_MAP["key"]] = item.key
    if "topic" in metadata_fields:
        row[_METADATA_COLUMN_MAP["topic"]] = topic
    if "partition" in metadata_fields:
        row[_METADATA_COLUMN_MAP["partition"]] = partition
    if "offset" in metadata_fields:
        row[_METADATA_COLUMN_MAP["offset"]] = item.offset
    if "headers" in metadata_fields:
        row[_METADATA_COLUMN_MAP["headers"]] = _serialize_headers(item.headers)
    if "timestamp" in metadata_fields:
        row[_METADATA_COLUMN_MAP["timestamp"]] = datetime.fromtimestamp(
            item.timestamp / 1000, tz=timezone.utc
        )

    return row


def _serialize_headers(headers: HeadersTuples) -> dict[str, str | None]:
    if not headers:
        return {}
    result: dict[str, str | None] = {}
    for key, value in headers:
        if isinstance(value, bytes):
            result[key] = value.decode("utf-8", errors="replace")
        else:
            result[key] = str(value) if value is not None else None
    return result


def _orjson_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


_LABEL_INVALID_CHARS = re.compile(r"[^a-zA-Z0-9_]")


def _sanitize_label(name: str) -> str:
    return _LABEL_INVALID_CHARS.sub("_", name)


def _table_name_setter(
    table_name: Callable[[SinkItem], str] | str,
) -> Callable[[SinkItem], str]:
    if isinstance(table_name, str):
        return lambda sink_item: table_name
    return table_name
