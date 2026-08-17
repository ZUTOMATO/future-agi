#!/usr/bin/env python3
"""Run the current CH25 list builders against named read-only populations.

This is deliberately not an HTTP qualification harness.  It exists for the
case where a historical ClickHouse project still exists but its PostgreSQL
authorization rows no longer do.  The result is therefore current-source SQL
builder/selector evidence only; ``qualify.py`` remains the public API gate.

The runner is safe for the production read-replica/SOS role:

* it accepts only ``SELECT`` or ``WITH ... SELECT`` statements;
* it uses ``ClickHouseClient(server_enforced_readonly=True)`` so top-level
  performance ``SETTINGS`` are removed before transport;
* it verifies ``currentUser()`` and ``getSetting('readonly')`` before customer
  reads;
* it emits digests, never discovered customer attribute/model values; and
* each lane has a 9.8 second process wall in addition to the application
  reader's 9.5 second wall.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import socket
import sys
import time
import traceback
import types
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SOURCE_ROOT_DEFAULT = "/tmp/current"
EXPECTED_READONLY_USER = "th7247_prod_readonly"
LANE_WALL_MS = 9_800
APPLICATION_WALL_MS = 9_500
PAGE_SIZE = 5

WINDOWS: tuple[tuple[str, timedelta], ...] = (
    ("30m", timedelta(minutes=30)),
    ("1h", timedelta(hours=1)),
    ("6h", timedelta(hours=6)),
    ("24h", timedelta(hours=24)),
    ("7d", timedelta(days=7)),
    ("30d", timedelta(days=30)),
    ("90d", timedelta(days=90)),
    ("180d", timedelta(days=180)),
    ("365d", timedelta(days=365)),
)
TARGETS: dict[str, dict[str, str]] = {
    "whatfix": {
        "project_id": "4b3d0477-ff0f-4681-9535-9b152152bf25",
        "density": "dense",
        "key": "whatfix.ent_id",
    },
    "colektia": {
        "project_id": "ca3025a9-b5eb-4872-9973-2330956d40d2",
        "density": "sparse",
        "key": "final_status",
        "preferred_value": "Rechazado",
    },
}
KINDS = ("trace", "span", "session")
PROFILES = ("default", "custom", "system", "combined")


class LaneWallExceeded(TimeoutError):
    """One matrix lane exceeded the outer 9.8 second wall."""


@contextmanager
def lane_wall() -> Any:
    previous = signal.getsignal(signal.SIGALRM)

    def expire(_signum: int, _frame: Any) -> None:
        raise LaneWallExceeded("lane wall exceeded")

    signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, LANE_WALL_MS / 1000)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def digest(value: Any) -> str:
    payload = json.dumps(
        value,
        default=str,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def parse_end(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(tzinfo=None)


def selected_values(raw: str, allowed: tuple[str, ...], label: str) -> tuple[str, ...]:
    requested = tuple(
        dict.fromkeys(part.strip() for part in raw.split(",") if part.strip())
    )
    unknown = sorted(set(requested) - set(allowed))
    if not requested or unknown:
        raise ValueError(f"invalid {label}: {unknown or raw}")
    return requested


def time_filter(start: datetime, end: datetime) -> dict[str, Any]:
    return {
        "column_id": "created_at",
        "filter_config": {
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [start.isoformat(), end.isoformat()],
            "col_type": "SYSTEM_METRIC",
        },
    }


def custom_filter(key: str, value: str) -> dict[str, Any]:
    return {
        "column_id": key,
        "filter_config": {
            "col_type": "SPAN_ATTRIBUTE",
            "filter_type": "text",
            "filter_op": "in",
            "filter_value": [value],
            "attribute_value_types": ["string"],
        },
    }


def system_filter(value: str) -> dict[str, Any]:
    return {
        "column_id": "model",
        "property_id": "system_attribute:traces:model",
        "source": "traces",
        "filter_config": {
            "col_type": "SYSTEM_METRIC",
            "filter_type": "text",
            "filter_op": "in",
            "filter_value": [value],
        },
    }


def profile_filters(
    profile: str,
    *,
    start: datetime,
    end: datetime,
    key: str,
    custom_value: str,
    model_value: str,
) -> list[dict[str, Any]]:
    result = [time_filter(start, end)]
    if profile in {"custom", "combined"}:
        result.append(custom_filter(key, custom_value))
    if profile in {"system", "combined"}:
        result.append(system_filter(model_value))
    return result


def install_current_source(source_root: str) -> None:
    root = str(Path(source_root).resolve())
    if not Path(root, "tracer").is_dir():
        raise RuntimeError("current tracer source root is missing")
    sys.path.insert(0, root)

    # Importing ``tracer.selectors.trace_filter_reads`` normally executes the
    # selector package's ORM-oriented re-exports.  This CH-only harness neither
    # needs nor permits PostgreSQL setup, so install a narrow namespace package
    # for the one selector module under test.
    selector_package = types.ModuleType("tracer.selectors")
    selector_package.__path__ = [str(Path(root, "tracer", "selectors"))]
    sys.modules["tracer.selectors"] = selector_package

    from django.conf import settings

    if not settings.configured:
        settings.configure(CLICKHOUSE={})


def install_socket_guard(host: str, port: int) -> None:
    original_connect = socket.socket.connect

    def guarded_connect(sock: socket.socket, address: Any) -> Any:
        if (
            not isinstance(address, tuple)
            or str(address[0]) != host
            or int(address[1]) != port
        ):
            raise RuntimeError("matrix network destination blocked")
        return original_connect(sock, address)

    socket.socket.connect = guarded_connect


@dataclass
class PageResult:
    record: dict[str, Any]
    rows: list[dict[str, Any]]
    bounded_page: Any = None


class ReadOnlyExecutor:
    """Selector-compatible executor with native progress accounting."""

    supports_per_query_read_settings = False

    def __init__(
        self,
        client: Any,
        query_result_class: Any,
        ensure_read_statement: Any,
        top_level_tokens: Any,
    ):
        self.client = client
        self.query_result_class = query_result_class
        self.ensure_read_statement = ensure_read_statement
        self.top_level_tokens = top_level_tokens
        self.calls: list[dict[str, Any]] = []

    def reset(self) -> None:
        self.calls.clear()

    def execute_ch_query(
        self,
        query: str,
        params: dict[str, Any],
        *,
        timeout_ms: int,
        settings: dict[str, Any],
    ) -> Any:
        statement_tokens = [
            token for token, _, _ in self.top_level_tokens(query) if token != ";"
        ]
        if not statement_tokens or statement_tokens[0] not in {"SELECT", "WITH"}:
            raise RuntimeError("matrix rejected a non-SELECT statement")
        self.ensure_read_statement(query)
        started = time.monotonic()
        rows, columns, _query_ms, read_rows, read_bytes = (
            self.client.execute_read_with_progress(
                query,
                params,
                timeout_ms=min(APPLICATION_WALL_MS, max(1, int(timeout_ms))),
                settings=settings,
            )
        )
        elapsed_ms = round((time.monotonic() - started) * 1000, 2)
        names = [item[0] if isinstance(item, tuple) else item for item in columns]
        data = [dict(zip(names, row, strict=False)) for row in rows]
        self.calls.append(
            {
                "wall_ms": elapsed_ms,
                "read_rows": int(read_rows or 0),
                "read_bytes": int(read_bytes or 0),
                "result_rows": len(data),
            }
        )
        return self.query_result_class(
            data=data,
            row_count=len(data),
            backend_used="clickhouse",
            query_time_ms=elapsed_ms,
            columns=names,
        )

    def metrics(self) -> dict[str, Any]:
        return {
            "query_count": len(self.calls),
            "ch_read_rows": sum(item["read_rows"] for item in self.calls),
            "ch_read_bytes": sum(item["read_bytes"] for item in self.calls),
            "slowest_query_ms": max(
                (item["wall_ms"] for item in self.calls), default=0.0
            ),
        }


def row_order(kind: str, row: dict[str, Any]) -> tuple[datetime | None, Any]:
    if kind == "trace":
        return row.get("start_time"), str(row.get("trace_id") or "")
    if kind == "span":
        return (
            row.get("start_time"),
            (
                str(row.get("id") or ""),
                str(row.get("trace_id") or ""),
                str(row.get("project_id") or ""),
            ),
        )
    return (
        row.get("_seed_order_start") or row.get("start_time"),
        str(row.get("_seed_order_id") or row.get("session_id") or ""),
    )


def row_id(kind: str, row: dict[str, Any]) -> Any:
    if kind == "trace":
        return row.get("project_id"), row.get("trace_id")
    if kind == "span":
        return row.get("project_id"), row.get("trace_id"), row.get("id")
    return row.get("project_id"), row.get("session_id")


def sanitized_error(exc: Exception) -> dict[str, Any]:
    frames = traceback.extract_tb(exc.__traceback__)
    final_frame = frames[-1] if frames else None
    return {
        "error_class": type(exc).__name__,
        "error_code": getattr(exc, "code", None),
        "error_frame": (
            f"{Path(final_frame.filename).name}:{final_frame.lineno}:{final_frame.name}"
            if final_frame is not None
            else None
        ),
        "error_message_sha256": digest(str(exc)),
    }


def build_builder(
    builder_class: Any,
    *,
    project_id: str,
    filters: list[dict[str, Any]],
    page_number: int = 0,
) -> Any:
    return builder_class(
        project_id=project_id,
        filters=filters,
        page_number=page_number,
        page_size=PAGE_SIZE,
        eval_config_ids=[],
        annotation_label_ids=[],
        bounded_internal_scan=True,
    )


def execute_page(
    *,
    kind: str,
    builder_class: Any,
    project_id: str,
    filters: list[dict[str, Any]],
    executor: ReadOnlyExecutor,
    read_bounded_filter_page: Any,
    continuation: PageResult | None = None,
) -> PageResult:
    executor.reset()
    started = time.monotonic()
    builder = build_builder(
        builder_class,
        project_id=project_id,
        filters=filters,
    )

    try:
        with lane_wall():
            # Mirror the public cursor-mode route in TraceSessionView. The
            # ordinary default session list deliberately uses the bounded
            # rollup seed plus finite exact classification; only the narrow
            # positive-user shape is safe for the direct candidate cursor.
            candidate_cursor = bool(
                kind == "session" and builder.supports_candidate_cursor_page()
            )
            if candidate_cursor:
                if continuation is not None and continuation.rows:
                    before_start, before_id = row_order(kind, continuation.rows[-1])
                else:
                    before_start = None
                    before_id = None
                query, params = builder.build_candidate_cursor_page_query(
                    before_start_time=before_start,
                    before_session_id=before_id,
                )
                query_result = executor.execute_ch_query(
                    query,
                    params,
                    timeout_ms=APPLICATION_WALL_MS,
                    settings={},
                )
                raw_rows = list(query_result.data or [])
                rows = raw_rows[:PAGE_SIZE]
                bounded_page = None
                complete = True
                has_more = len(raw_rows) > PAGE_SIZE
                status = "complete"
                error_code = None
            else:
                kwargs: dict[str, Any] = {
                    "builder": builder,
                    "analytics": executor,
                    "filters": filters,
                    "key_field": {
                        "trace": "trace_id",
                        "span": "id",
                        "session": "session_id",
                    }[kind],
                    "page_number": 0,
                    "page_size": PAGE_SIZE,
                    "deadline_ms": APPLICATION_WALL_MS,
                    "read_settings": {},
                    "include_incomplete_rows": True,
                    "bounded_continuation": True,
                }
                if kind == "session":
                    kwargs.update(
                        max_candidates=200,
                        max_seed_attempts=24,
                        max_query_count=48,
                        classify_batch_size=builder.recommended_filter_classify_batch_size(),
                    )
                if continuation is not None and continuation.bounded_page is not None:
                    prior = continuation.bounded_page
                    if continuation.rows:
                        cursor_time, cursor_token = row_order(
                            kind, continuation.rows[-1]
                        )
                    else:
                        cursor_time = (
                            prior.continuation_before_start_time
                            or prior.continuation_slice_end
                        )
                        cursor_token = prior.continuation_before_id
                    if cursor_time is not None and cursor_token is not None:
                        kwargs.update(
                            cursor_start_time=cursor_time,
                            cursor_order_token=cursor_token,
                        )
                    if not prior.has_more:
                        kwargs.update(
                            continuation_slice_start=prior.continuation_slice_start,
                            continuation_slice_end=prior.continuation_slice_end,
                            continuation_before_start_time=(
                                prior.continuation_before_start_time
                            ),
                            continuation_before_id=prior.continuation_before_id,
                        )
                bounded_page = read_bounded_filter_page(**kwargs)
                rows = list(bounded_page.rows or [])
                complete = bool(bounded_page.complete)
                has_more = bool(
                    bounded_page.has_more
                    or bounded_page.continuation_slice_end is not None
                )
                status = str(bounded_page.status)
                error_code = bounded_page.error_code

        elapsed_ms = round((time.monotonic() - started) * 1000, 2)
        record = {
            "wall_ms": elapsed_ms,
            "within_10s": elapsed_ms < 10_000,
            "status": status,
            "complete": complete,
            "has_more": has_more,
            "error_code": error_code,
            "row_count": len(rows),
            "row_digest": digest([row_id(kind, row) for row in rows]),
            **executor.metrics(),
        }
        return PageResult(record=record, rows=rows, bounded_page=bounded_page)
    except Exception as exc:
        elapsed_ms = round((time.monotonic() - started) * 1000, 2)
        return PageResult(
            record={
                "wall_ms": elapsed_ms,
                "within_10s": elapsed_ms < 10_000,
                "status": "failed",
                "complete": False,
                "has_more": False,
                "row_count": 0,
                "row_digest": digest([]),
                **executor.metrics(),
                **sanitized_error(exc),
            },
            rows=[],
        )


def discover_value(
    *,
    executor: ReadOnlyExecutor,
    project_id: str,
    key: str,
    start: datetime,
    end: datetime,
    preferred_value: str | None,
) -> tuple[str, dict[str, Any]]:
    executor.reset()
    query = """
        SELECT attrs_string[%(key)s] AS value
        FROM spans
        PREWHERE project_id = toUUID(%(project_id)s)
          AND start_time >= %(start)s
          AND start_time < %(end)s
        WHERE is_deleted = 0
          AND indexHint(has(mapKeys(attrs_string), %(key)s))
          AND has(attrs_string.keys, %(key)s)
          AND notEmpty(attrs_string[%(key)s])
          AND (%(preferred)s = '' OR attrs_string[%(key)s] = %(preferred)s)
        LIMIT 1
    """
    started = time.monotonic()
    with lane_wall():
        result = executor.execute_ch_query(
            query,
            {
                "project_id": project_id,
                "key": key,
                "start": start,
                "end": end,
                "preferred": preferred_value or "",
            },
            timeout_ms=APPLICATION_WALL_MS,
            settings={},
        )
    if not result.data:
        raise RuntimeError("custom profile population was empty")
    value = str(result.data[0]["value"])
    return value, {
        "wall_ms": round((time.monotonic() - started) * 1000, 2),
        "value_sha256": digest(value),
        "population_lower_bound": 1,
        **executor.metrics(),
    }


def discover_model(
    *,
    executor: ReadOnlyExecutor,
    project_id: str,
    start: datetime,
    end: datetime,
) -> tuple[str, dict[str, Any]]:
    executor.reset()
    query = """
        SELECT model AS value
        FROM spans
        PREWHERE project_id = toUUID(%(project_id)s)
          AND start_time >= %(start)s
          AND start_time < %(end)s
        WHERE is_deleted = 0 AND notEmpty(model)
        LIMIT 1
    """
    started = time.monotonic()
    with lane_wall():
        result = executor.execute_ch_query(
            query,
            {"project_id": project_id, "start": start, "end": end},
            timeout_ms=APPLICATION_WALL_MS,
            settings={},
        )
    if not result.data:
        raise RuntimeError("system Model profile population was empty")
    value = str(result.data[0]["value"])
    return value, {
        "wall_ms": round((time.monotonic() - started) * 1000, 2),
        "value_sha256": digest(value),
        "population_lower_bound": 1,
        **executor.metrics(),
    }


def emit(record: dict[str, Any], records: list[dict[str, Any]]) -> None:
    records.append(record)
    print(json.dumps(record, sort_keys=True, separators=(",", ":")), flush=True)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--source-root", default=SOURCE_ROOT_DEFAULT)
    result.add_argument("--source-sha", required=True)
    result.add_argument("--end", required=True)
    result.add_argument("--targets", default=",".join(TARGETS))
    result.add_argument("--windows", default=",".join(name for name, _ in WINDOWS))
    result.add_argument("--profiles", default=",".join(PROFILES))
    result.add_argument("--kinds", default=",".join(KINDS))
    result.add_argument("--continuation-window", default="365d")
    result.add_argument("--output")
    result.add_argument("--plan", action="store_true")
    return result


def main() -> int:
    args = parser().parse_args()
    targets = selected_values(args.targets, tuple(TARGETS), "targets")
    windows = selected_values(
        args.windows, tuple(name for name, _ in WINDOWS), "windows"
    )
    profiles = selected_values(args.profiles, PROFILES, "profiles")
    kinds = selected_values(args.kinds, KINDS, "kinds")
    end = parse_end(args.end)
    plan = [
        (target, window, profile, kind)
        for target in targets
        for window in windows
        for profile in profiles
        for kind in kinds
    ]
    if args.plan:
        print(
            json.dumps(
                {
                    "event": "plan",
                    "source_sha": args.source_sha,
                    "end": end.isoformat(),
                    "lane_count": len(plan),
                    "targets": targets,
                    "windows": windows,
                    "profiles": profiles,
                    "kinds": kinds,
                },
                sort_keys=True,
            )
        )
        return 0

    install_current_source(args.source_root)

    import structlog

    logging.disable(logging.CRITICAL)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.CRITICAL)
    )

    from tracer.selectors.trace_filter_reads import read_bounded_filter_page
    from tracer.services.clickhouse.client import ClickHouseClient
    from tracer.services.clickhouse.query_service import QueryResult
    from tracer.services.clickhouse.server_readonly import (
        _top_level_tokens,
        ensure_read_statement,
    )
    from tracer.services.clickhouse.v2.query_builders.session_list import (
        SessionListQueryBuilderV2,
    )
    from tracer.services.clickhouse.v2.query_builders.span_list import (
        SpanListQueryBuilderV2,
    )
    from tracer.services.clickhouse.v2.query_builders.trace_list import (
        TraceListQueryBuilderV2,
    )

    host = os.environ["CH_HOST"]
    port = int(os.environ["CH_PORT"])
    username = os.environ["CH_USERNAME"]
    if username != EXPECTED_READONLY_USER:
        raise RuntimeError("unexpected ClickHouse role")
    install_socket_guard(host, port)
    client = ClickHouseClient(
        host=host,
        port=port,
        user=username,
        password=os.environ["CH_PASSWORD"],
        database=os.environ["CH_DATABASE"],
        server_enforced_readonly=True,
        connect_timeout=5,
        send_timeout=10,
        receive_timeout=10,
        pool_size=1,
        read_timeout_ceiling_ms=APPLICATION_WALL_MS,
    )
    executor = ReadOnlyExecutor(
        client,
        QueryResult,
        ensure_read_statement,
        _top_level_tokens,
    )
    records: list[dict[str, Any]] = []

    executor.reset()
    attestation = executor.execute_ch_query(
        "SELECT currentUser() AS current_user, "
        "toUInt64(getSetting('readonly')) AS readonly",
        {},
        timeout_ms=2_000,
        settings={},
    )
    if (
        len(attestation.data) != 1
        or attestation.data[0].get("current_user") != EXPECTED_READONLY_USER
        or int(attestation.data[0].get("readonly") or 0) != 1
    ):
        raise RuntimeError("server read-only attestation failed")
    emit(
        {
            "event": "matrix_header",
            "source_sha": args.source_sha,
            "end": end.isoformat(),
            "readonly_attested": True,
            "lane_wall_ms": LANE_WALL_MS,
            "application_wall_ms": APPLICATION_WALL_MS,
            "lane_count": len(plan),
            "continuation_window": args.continuation_window,
        },
        records,
    )

    window_by_name = dict(WINDOWS)
    builder_by_kind = {
        "trace": TraceListQueryBuilderV2,
        "span": SpanListQueryBuilderV2,
        "session": SessionListQueryBuilderV2,
    }
    discovered: dict[str, tuple[str, str]] = {}
    for target_name in targets:
        spec = TARGETS[target_name]
        start = end - window_by_name["365d"]
        custom_value, custom_metrics = discover_value(
            executor=executor,
            project_id=spec["project_id"],
            key=spec["key"],
            start=start,
            end=end,
            preferred_value=spec.get("preferred_value"),
        )
        model_value, model_metrics = discover_model(
            executor=executor,
            project_id=spec["project_id"],
            start=start,
            end=end,
        )
        discovered[target_name] = custom_value, model_value
        emit(
            {
                "event": "profile_discovery",
                "target": target_name,
                "density": spec["density"],
                "custom": custom_metrics,
                "system": model_metrics,
            },
            records,
        )

    lane_records: list[dict[str, Any]] = []
    for target_name, window_name, profile, kind in plan:
        spec = TARGETS[target_name]
        custom_value, model_value = discovered[target_name]
        filters = profile_filters(
            profile,
            start=end - window_by_name[window_name],
            end=end,
            key=spec["key"],
            custom_value=custom_value,
            model_value=model_value,
        )
        first = execute_page(
            kind=kind,
            builder_class=builder_by_kind[kind],
            project_id=spec["project_id"],
            filters=filters,
            executor=executor,
            read_bounded_filter_page=read_bounded_filter_page,
        )
        lane = {
            "event": "lane",
            "target": target_name,
            "density": spec["density"],
            "window": window_name,
            "profile": profile,
            "kind": kind,
            "page": "p1",
            **first.record,
        }
        lane_records.append(lane)
        emit(lane, records)

        if window_name != args.continuation_window:
            continue
        repeat = execute_page(
            kind=kind,
            builder_class=builder_by_kind[kind],
            project_id=spec["project_id"],
            filters=filters,
            executor=executor,
            read_bounded_filter_page=read_bounded_filter_page,
        )
        repeat_lane = {
            "event": "lane",
            "target": target_name,
            "density": spec["density"],
            "window": window_name,
            "profile": profile,
            "kind": kind,
            "page": "p1_repeat",
            "stable_repeat": (
                first.record.get("status") == repeat.record.get("status")
                and first.record.get("row_digest") == repeat.record.get("row_digest")
            ),
            **repeat.record,
        }
        lane_records.append(repeat_lane)
        emit(repeat_lane, records)

        if not first.record.get("has_more"):
            continue
        second = execute_page(
            kind=kind,
            builder_class=builder_by_kind[kind],
            project_id=spec["project_id"],
            filters=filters,
            executor=executor,
            read_bounded_filter_page=read_bounded_filter_page,
            continuation=first,
        )
        second_ids = {row_id(kind, row) for row in second.rows}
        first_ids = {row_id(kind, row) for row in first.rows}
        second_lane = {
            "event": "lane",
            "target": target_name,
            "density": spec["density"],
            "window": window_name,
            "profile": profile,
            "kind": kind,
            "page": "p2",
            "cursor_disjoint": not bool(first_ids & second_ids),
            **second.record,
        }
        lane_records.append(second_lane)
        emit(second_lane, records)

    failed = [item for item in lane_records if item.get("status") == "failed"]
    over_wall = [item for item in lane_records if not item.get("within_10s")]
    unstable = [
        item
        for item in lane_records
        if item.get("page") == "p1_repeat" and item.get("stable_repeat") is not True
    ]
    overlap = [
        item
        for item in lane_records
        if item.get("page") == "p2" and item.get("cursor_disjoint") is not True
    ]
    summary = {
        "event": "matrix_summary",
        "source_sha": args.source_sha,
        "lane_count": len(lane_records),
        "failed_count": len(failed),
        "over_10s_count": len(over_wall),
        "unstable_repeat_count": len(unstable),
        "cursor_overlap_count": len(overlap),
        "slowest_wall_ms": max(
            (float(item.get("wall_ms") or 0) for item in lane_records), default=0.0
        ),
        "qualified": not (failed or over_wall or unstable or overlap),
        "failed_lanes": [
            ".".join(
                str(item.get(key) or "")
                for key in ("target", "window", "profile", "kind", "page")
            )
            for item in [*failed, *over_wall, *unstable, *overlap]
        ],
    }
    emit(summary, records)
    if args.output:
        Path(args.output).write_text(
            json.dumps(records, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
    return 0 if summary["qualified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
