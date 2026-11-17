"""数据湖核心：结构版本、字段身份、批次登记与不可变 Parquet。

设计要点
--------
* 测量列的物理标识是永久不变的 ``field_id``；Parquet 中保存的就是 field_id。
  列名只是 ``fields`` 表上的一个可修改属性。
* 删除字段仅把状态置为 deleted。之后用同名新增会得到新的 field_id，
  旧批次里没有新 id 的列，读取时补 NULL，因此不会串值。
* 结构变更与批次登记都在 SQLite ``BEGIN IMMEDIATE`` 事务中完成，并校验
  调用方看到的 schema_version；过期则整次拒绝。
* 新 Parquet 先写临时文件、fsync、rename，再登记数据库。数据库提交失败会
  删除刚写的文件。历史 Parquet 永不以写方式打开。
* 一次页面/API 查询只在一个 SQLite 读事务里取得“结构 + 批次登记”快照。
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import re
import sqlite3
import time
import unicodedata
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("metlake")

MAX_FIELDS = 8
MAX_ROWS = 1000
MAX_UPLOAD_BYTES = 5 * 1024 * 1024
MAX_STATION_LENGTH = 256
MAX_FIELD_NAME_LENGTH = 64

STATION_COLUMN = "station_id"
TIME_COLUMN = "observed_at"
RESERVED_NAMES = {STATION_COLUMN.casefold(), TIME_COLUMN.casefold()}
FIELD_ID_RE = re.compile(r"^f_[0-9a-f]{32}$")
INT_RE = re.compile(r"^[+-]?\d+$")
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

SCHEMA_VERSION_KEY = "schema_version"
EVENT_JOURNAL_START_KEY = "event_journal_start"
EVENT_JOURNAL_START_AT_KEY = "event_journal_start_at"
UPLOAD_METADATA_PREFIX = b"metlake."

# 变更事件订阅（append-only journal）
EVENT_DEFAULT_LIMIT = 50
EVENT_MAX_LIMIT = 200
CURSOR_RE = re.compile(r"^\d+$")
EVENT_LABELS = {
    "field_added": "新增字段",
    "field_renamed": "字段改名",
    "field_deleted": "删除字段",
    "batch_registered": "批次登记",
}
EVENT_TYPES = frozenset(EVENT_LABELS)


class LakeError(Exception):
    """可安全展示给用户的领域错误。"""

    status_code = 400
    code = "bad_request"


class ValidationError(LakeError):
    status_code = 400
    code = "validation"


class StaleSchema(LakeError):
    status_code = 409
    code = "stale_schema"

    def __init__(self, expected: int, actual: int) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"结构版本已过期：本次操作基于 v{expected}，当前已提交结构为 v{actual}。"
            "为避免旧结构写入，整次操作已被拒绝，请刷新页面后重试。"
        )


class NotFound(LakeError):
    status_code = 404
    code = "not_found"


class StorageError(LakeError):
    status_code = 500
    code = "storage_error"


class BatchIntegrityError(LakeError):
    status_code = 500
    code = "integrity_error"


class InvalidCursor(LakeError):
    status_code = 400
    code = "invalid_cursor"


# ---------------------------------------------------------------------------
# 配置 / 数据库基础
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    instance_name: str = "local"

    @classmethod
    def from_env(
        cls,
        data_dir: str | os.PathLike[str] | None = None,
        instance_name: str | None = None,
    ) -> "Settings":
        resolved = Path(data_dir or os.environ.get("DATA_DIR", "./data")).expanduser()
        name = instance_name or os.environ.get("INSTANCE_NAME", "local")
        return cls(data_dir=resolved.resolve(), instance_name=name)

    @property
    def db_path(self) -> Path:
        return self.data_dir / "lake.sqlite3"

    @property
    def parquet_dir(self) -> Path:
        return self.data_dir / "parquet"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Field:
    id: str
    name: str
    ordinal: int
    status: str
    created_version: int
    deleted_version: int | None


@dataclass(frozen=True)
class Schema:
    version: int
    fields: tuple[Field, ...]

    @property
    def active(self) -> tuple[Field, ...]:
        return tuple(f for f in self.fields if f.status == "active")

    @property
    def deleted(self) -> tuple[Field, ...]:
        return tuple(f for f in self.fields if f.status == "deleted")


@dataclass(frozen=True)
class Batch:
    id: str
    created_at: str
    schema_version: int
    row_count: int
    file_name: str
    file_sha256: str
    fields_at_upload: tuple[dict, ...]

    @property
    def short_id(self) -> str:
        return self.id[:8]


@dataclass(frozen=True)
class LakeState:
    schema: Schema
    batches: tuple[Batch, ...]


@dataclass(frozen=True)
class BatchView:
    schema: Schema
    batch: Batch
    table: pa.Table


@dataclass(frozen=True)
class ParsedCsv:
    stations: tuple[str, ...]
    times: tuple[datetime, ...]
    values: dict[str, tuple[int | None, ...]]
    header_fields: tuple[Field, ...]


@dataclass(frozen=True)
class IntegrityReport:
    ok: bool
    batches: int
    missing_files: tuple[str, ...]
    hash_mismatches: tuple[str, ...]
    orphan_files: tuple[str, ...]
    temp_files: tuple[str, ...]
    event_gaps: tuple[int, ...] = ()


@dataclass(frozen=True)
class ChangeEvent:
    """一条已提交的变更事件；``id`` 永久单调递增，就是分页游标。"""

    id: int
    type: str
    schema_version: int
    created_at: str
    field_id: str | None
    field_name: str | None
    old_name: str | None
    new_name: str | None
    structure_version: int | None
    batch_id: str | None
    batch_row_count: int | None
    batch_file_name: str | None
    batch_fields: tuple[dict, ...]
    source: str | None


@dataclass(frozen=True)
class EventPage:
    events: tuple[ChangeEvent, ...]
    next_cursor: str
    has_more: bool
    limit: int
    journal_start_version: int = 0
    latest_event_id: int = 0
    total_events: int = 0


# 仅测试使用：Parquet 已写完、数据库提交前的钩子。
_before_commit_hook: Callable[[], None] | None = None


def set_before_commit_hook(hook: Callable[[], None] | None) -> None:
    """测试钩子：在转换完成、批次登记提交前执行。生产路径不设置。"""

    global _before_commit_hook
    _before_commit_hook = hook


def _connect(settings: Settings) -> sqlite3.Connection:
    conn = sqlite3.connect(
        settings.db_path,
        timeout=30.0,
        isolation_level=None,  # 手动控制事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 15000")
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize(settings: Settings) -> None:
    """幂等初始化；两个实例同时启动也安全。"""

    settings.ensure_dirs()
    last_error: Exception | None = None
    for attempt in range(60):
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(
                settings.db_path,
                timeout=30.0,
                isolation_level=None,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout = 15000")
            mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                log.warning("SQLite journal_mode=%s (期望 wal)", mode)
            conn.execute("PRAGMA synchronous = FULL")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fields (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('active', 'deleted')),
                    created_version INTEGER NOT NULL,
                    deleted_version INTEGER,
                    created_at TEXT NOT NULL,
                    deleted_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_fields_active_name
                ON fields(name COLLATE NOCASE)
                WHERE status = 'active'
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_fields_status_ordinal "
                "ON fields(status, ordinal)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    row_count INTEGER NOT NULL,
                    file_name TEXT NOT NULL UNIQUE,
                    file_sha256 TEXT NOT NULL,
                    schema_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_batches_created_at "
                "ON batches(created_at DESC)"
            )
            _init_event_journal(conn)
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES (?, '0')",
                (SCHEMA_VERSION_KEY,),
            )
            conn.execute("COMMIT")
            _cleanup_stale_temps(settings)
            _cleanup_orphan_files(settings)
            return
        except sqlite3.Error as exc:  # 并发启动时可能 busy
            last_error = exc
            if conn is not None:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            time.sleep(min(0.05 * (attempt + 1), 0.5))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
    raise StorageError(
        f"无法初始化数据库 {settings.db_path}：{last_error}"
    ) from last_error


def _init_event_journal(conn: sqlite3.Connection) -> None:
    """建事件表并记录订阅起点（幂等）。

    事件表只追加：ID 由 SQLite AUTOINCREMENT 分配，永不回收、永不改写。
    只有订阅启用之后的提交才写事件；升级前已存在的字段与批次**不补写**，
    因为历史改名无法还原，补写会伪造“变更前名称”。``event_journal_start``
    记录启用时的结构版本，订阅 API 会返回该边界。
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL CHECK (
                type IN ('field_added', 'field_renamed',
                         'field_deleted', 'batch_registered')
            ),
            schema_version INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            field_id TEXT,
            field_name TEXT,
            old_name TEXT,
            new_name TEXT,
            structure_version INTEGER,
            batch_id TEXT,
            batch_row_count INTEGER,
            batch_file_name TEXT,
            batch_fields_json TEXT,
            source TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_events_schema_version "
        "ON events(schema_version)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_events_batch_id ON events(batch_id)"
    )
    # 数据库层保证事件不可改写 / 不可删除，即使绕过应用代码也一样。
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_events_append_only_update
        BEFORE UPDATE ON events
        BEGIN
            SELECT RAISE(ABORT, 'events are append-only');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_events_append_only_delete
        BEFORE DELETE ON events
        BEGIN
            SELECT RAISE(ABORT, 'events are append-only');
        END
        """
    )
    if conn.execute(
        "SELECT 1 FROM meta WHERE key = ?", (EVENT_JOURNAL_START_KEY,)
    ).fetchone() is not None:
        return
    version_row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (SCHEMA_VERSION_KEY,)
    ).fetchone()
    start_version = int(version_row["value"]) if version_row is not None else 0
    conn.execute(
        "INSERT INTO meta(key, value) VALUES (?, ?)",
        (EVENT_JOURNAL_START_KEY, str(start_version)),
    )
    conn.execute(
        "INSERT INTO meta(key, value) VALUES (?, ?)",
        (EVENT_JOURNAL_START_AT_KEY, _utc_now_iso()),
    )


@contextmanager
def _write_transaction(settings: Settings) -> Iterator[sqlite3.Connection]:
    conn = _connect(settings)
    committed = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
        committed = True
    except BaseException:
        if not committed:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        raise
    finally:
        try:
            conn.close()
        except sqlite3.Error:
            # 关闭失败不改变事务结果；已有提交/回滚结论。
            log.exception("关闭数据库连接失败")


@contextmanager
def _read_transaction(settings: Settings) -> Iterator[sqlite3.Connection]:
    conn = _connect(settings)
    try:
        conn.execute("BEGIN")
        yield conn
    finally:
        try:
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        conn.close()


def _rows_to_batch(row: sqlite3.Row) -> Batch:
    try:
        fields = tuple(json.loads(row["schema_json"]))
    except (TypeError, ValueError):
        fields = ()
    return Batch(
        id=row["id"],
        created_at=row["created_at"],
        schema_version=int(row["schema_version"]),
        row_count=int(row["row_count"]),
        file_name=row["file_name"],
        file_sha256=row["file_sha256"],
        fields_at_upload=fields,
    )


def _query_schema(conn: sqlite3.Connection) -> Schema:
    version_row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (SCHEMA_VERSION_KEY,)
    ).fetchone()
    if version_row is None:
        raise BatchIntegrityError("数据库缺少 schema_version")
    version = int(version_row["value"])
    rows = conn.execute(
        "SELECT id, name, ordinal, status, created_version, deleted_version "
        "FROM fields ORDER BY ordinal ASC, created_at ASC"
    ).fetchall()
    fields = tuple(
        Field(
            id=r["id"],
            name=r["name"],
            ordinal=int(r["ordinal"]),
            status=r["status"],
            created_version=int(r["created_version"]),
            deleted_version=(
                int(r["deleted_version"]) if r["deleted_version"] is not None else None
            ),
        )
        for r in rows
    )
    return Schema(version=version, fields=fields)


def read_state(settings: Settings) -> LakeState:
    """在一个读事务中取得一致的结构 + 批次登记快照。"""

    try:
        with _read_transaction(settings) as conn:
            schema = _query_schema(conn)
            rows = conn.execute(
                "SELECT id, created_at, schema_version, row_count, file_name, "
                "file_sha256, schema_json FROM batches "
                "ORDER BY created_at DESC, id DESC"
            ).fetchall()
            batches = tuple(_rows_to_batch(r) for r in rows)
    except sqlite3.Error as exc:
        raise StorageError("读取数据湖状态失败。") from exc
    return LakeState(schema=schema, batches=batches)


def read_schema(settings: Settings) -> Schema:
    try:
        with _read_transaction(settings) as conn:
            return _query_schema(conn)
    except sqlite3.Error as exc:
        raise StorageError("读取结构失败。") from exc


# ---------------------------------------------------------------------------
# 变更事件订阅（游标分页）
# ---------------------------------------------------------------------------


def normalize_cursor(raw: object) -> int:
    """游标 = 已消费的最后一条事件 ID；空值表示从头开始。"""

    if raw is None:
        return 0
    text = str(raw).strip()
    if not text:
        return 0
    if not CURSOR_RE.fullmatch(text):
        raise InvalidCursor("游标必须是非负整数（上页返回的 next_cursor）。")
    value = int(text)
    if value < 0:
        raise InvalidCursor("游标必须是非负整数。")
    return value


def normalize_limit(raw: object) -> int:
    if raw is None or str(raw).strip() == "":
        return EVENT_DEFAULT_LIMIT
    text = str(raw).strip()
    if not CURSOR_RE.fullmatch(text):
        raise ValidationError("limit 必须是正整数。")
    value = int(text)
    if value < 1:
        raise ValidationError("limit 必须是正整数。")
    if value > EVENT_MAX_LIMIT:
        raise ValidationError(f"limit 最大 {EVENT_MAX_LIMIT}。")
    return value


def _rows_to_event(row: sqlite3.Row) -> ChangeEvent:
    try:
        fields = tuple(json.loads(row["batch_fields_json"])) if row["batch_fields_json"] else ()
    except (TypeError, ValueError):
        fields = ()
    return ChangeEvent(
        id=int(row["id"]),
        type=row["type"],
        schema_version=int(row["schema_version"]),
        created_at=row["created_at"],
        field_id=row["field_id"],
        field_name=row["field_name"],
        old_name=row["old_name"],
        new_name=row["new_name"],
        structure_version=(
            int(row["structure_version"])
            if row["structure_version"] is not None
            else None
        ),
        batch_id=row["batch_id"],
        batch_row_count=(
            int(row["batch_row_count"])
            if row["batch_row_count"] is not None
            else None
        ),
        batch_file_name=row["batch_file_name"],
        batch_fields=fields,
        source=row["source"],
    )


def read_event(settings: Settings, event_id: int) -> ChangeEvent:
    if isinstance(event_id, bool) or not isinstance(event_id, int) or event_id < 1:
        raise NotFound("事件不存在。")
    try:
        with _read_transaction(settings) as conn:
            row = conn.execute(
                "SELECT id, type, schema_version, created_at, field_id, field_name, "
                "old_name, new_name, structure_version, batch_id, batch_row_count, "
                "batch_file_name, batch_fields_json, source FROM events WHERE id = ?",
                (event_id,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise StorageError("读取变更事件失败。") from exc
    if row is None:
        raise NotFound("事件不存在。")
    return _rows_to_event(row)


def read_events(
    settings: Settings,
    cursor: object = None,
    limit: object = None,
) -> EventPage:
    """读取一段连续事件：只返回 id > cursor 的前 ``limit`` 条。

    整个查询（含 next_cursor / latest_event_id）在一个读事务里完成，因此只会
    看到已提交事件，不会出现空洞或半页。新提交的事件只会在后续页出现，已返回
    事件不会被改写。``next_cursor`` 是本页最后一条事件的 ID，可直接作为下一页
    的 ``cursor``；用旧游标重试是幂等的。
    """

    limit_value = normalize_limit(limit)
    start = normalize_cursor(cursor)

    try:
        with _read_transaction(settings) as conn:
            rows = conn.execute(
                "SELECT id, type, schema_version, created_at, field_id, field_name, "
                "old_name, new_name, structure_version, batch_id, batch_row_count, "
                "batch_file_name, batch_fields_json, source FROM events "
                "WHERE id > ? ORDER BY id ASC LIMIT ?",
                (start, limit_value + 1),
            ).fetchall()
            start_row = conn.execute(
                "SELECT value FROM meta WHERE key = ?",
                (EVENT_JOURNAL_START_KEY,),
            ).fetchone()
            latest_row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) AS m FROM events"
            ).fetchone()
            total_row = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()
    except sqlite3.Error as exc:
        raise StorageError("读取变更事件失败。") from exc

    journal_start = int(start_row["value"]) if start_row is not None else 0
    has_more = len(rows) > limit_value
    events = tuple(_rows_to_event(r) for r in rows[:limit_value])
    next_cursor = str(events[-1].id) if events else str(start)
    return EventPage(
        events=events,
        next_cursor=next_cursor,
        has_more=has_more,
        limit=limit_value,
        journal_start_version=journal_start,
        latest_event_id=int(latest_row["m"]),
        total_events=int(total_row["n"]),
    )


# ---------------------------------------------------------------------------
# 字段结构变更
# ---------------------------------------------------------------------------


def normalize_field_name(raw: str | None) -> str:
    if raw is None:
        raise ValidationError("字段名不能为空。")
    name = unicodedata.normalize("NFC", str(raw).strip())
    if not name:
        raise ValidationError("字段名不能为空。")
    if len(name) > MAX_FIELD_NAME_LENGTH:
        raise ValidationError(f"字段名最长 {MAX_FIELD_NAME_LENGTH} 个字符。")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in name):
        raise ValidationError("字段名不能包含控制字符。")
    if name.casefold() in RESERVED_NAMES:
        raise ValidationError(
            f"“{name}”是固定列名（{STATION_COLUMN} / {TIME_COLUMN}），不能用作测量列名。"
        )
    return name


def _read_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (SCHEMA_VERSION_KEY,)
    ).fetchone()
    if row is None:
        raise BatchIntegrityError("数据库缺少 schema_version")
    return int(row["value"])


def _check_expected_version(conn: sqlite3.Connection, expected: int) -> int:
    actual = _read_version(conn)
    if expected != actual:
        raise StaleSchema(expected, actual)
    return actual


def _bump_version(conn: sqlite3.Connection, current: int) -> int:
    new_version = current + 1
    conn.execute(
        "UPDATE meta SET value = ? WHERE key = ?",
        (str(new_version), SCHEMA_VERSION_KEY),
    )
    return new_version


def _append_field_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    created_at: str,
    field_id: str,
    field_name: str,
    structure_version: int,
    old_name: str | None,
    new_name: str | None,
    source: str | None,
) -> None:
    """在调用方的事务内追加结构事件；与字段变更、版本号同生共死。"""

    conn.execute(
        "INSERT INTO events(type, schema_version, created_at, field_id, "
        "field_name, old_name, new_name, structure_version, source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event_type,
            structure_version,
            created_at,
            field_id,
            field_name,
            old_name,
            new_name,
            structure_version,
            source,
        ),
    )


def _append_batch_event(
    conn: sqlite3.Connection,
    *,
    created_at: str,
    schema_version: int,
    batch_id: str,
    row_count: int,
    file_name: str,
    schema_json: str,
    source: str | None,
) -> None:
    conn.execute(
        "INSERT INTO events(type, schema_version, created_at, batch_id, "
        "batch_row_count, batch_file_name, batch_fields_json, source) "
        "VALUES ('batch_registered', ?, ?, ?, ?, ?, ?, ?)",
        (
            schema_version,
            created_at,
            batch_id,
            row_count,
            file_name,
            schema_json,
            source,
        ),
    )


def _next_ordinal(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(ordinal), -1) AS m FROM fields").fetchone()
    return int(row["m"]) + 1


def _ensure_name_free(
    conn: sqlite3.Connection, name: str, exclude_id: str | None = None
) -> None:
    rows = conn.execute(
        "SELECT id, name FROM fields WHERE status = 'active'"
    ).fetchall()
    folded = name.casefold()
    for row in rows:
        if exclude_id is not None and row["id"] == exclude_id:
            continue
        if row["name"].casefold() == folded:
            raise ValidationError(f"测量列“{name}”已存在。")


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def add_field(
    settings: Settings,
    expected_version: int,
    raw_name: str,
    *,
    source: str | None = None,
) -> Schema:
    name = normalize_field_name(raw_name)
    try:
        with _write_transaction(settings) as conn:
            version = _check_expected_version(conn, expected_version)
            active = conn.execute(
                "SELECT COUNT(*) AS n FROM fields WHERE status = 'active'"
            ).fetchone()["n"]
            if int(active) >= MAX_FIELDS:
                raise ValidationError(
                    f"测量列最多 {MAX_FIELDS} 个，请先删除不再使用的列。"
                )
            _ensure_name_free(conn, name)
            new_version = _bump_version(conn, version)
            now = _utc_now_iso()
            field_id = f"f_{uuid.uuid4().hex}"
            conn.execute(
                "INSERT INTO fields(id, name, ordinal, status, created_version, "
                "deleted_version, created_at, deleted_at) "
                "VALUES (?, ?, ?, 'active', ?, NULL, ?, NULL)",
                (
                    field_id,
                    name,
                    _next_ordinal(conn),
                    new_version,
                    now,
                ),
            )
            _append_field_event(
                conn,
                event_type="field_added",
                created_at=now,
                field_id=field_id,
                field_name=name,
                structure_version=new_version,
                old_name=None,
                new_name=name,
                source=source,
            )
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("新增测量列失败，结构未改变。") from exc
    return read_schema(settings)


def rename_field(
    settings: Settings,
    expected_version: int,
    field_id: str,
    raw_name: str,
    *,
    source: str | None = None,
) -> Schema:
    name = normalize_field_name(raw_name)
    try:
        with _write_transaction(settings) as conn:
            version = _check_expected_version(conn, expected_version)
            row = conn.execute(
                "SELECT id, name FROM fields WHERE id = ? AND status = 'active'",
                (field_id,),
            ).fetchone()
            if row is None:
                raise NotFound("要改名的测量列不存在或已删除。")
            if row["name"] == name:
                raise ValidationError("新名称与当前名称相同。")
            _ensure_name_free(conn, name, exclude_id=field_id)
            new_version = _bump_version(conn, version)
            now = _utc_now_iso()
            conn.execute(
                "UPDATE fields SET name = ? WHERE id = ?", (name, field_id)
            )
            _append_field_event(
                conn,
                event_type="field_renamed",
                created_at=now,
                field_id=field_id,
                field_name=name,
                structure_version=new_version,
                old_name=row["name"],
                new_name=name,
                source=source,
            )
            log.info("renamed %s -> %s at v%s", field_id, name, new_version)
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("测量列改名失败，结构未改变。") from exc
    return read_schema(settings)


def delete_field(
    settings: Settings,
    expected_version: int,
    field_id: str,
    *,
    source: str | None = None,
) -> Schema:
    try:
        with _write_transaction(settings) as conn:
            version = _check_expected_version(conn, expected_version)
            row = conn.execute(
                "SELECT id, name FROM fields WHERE id = ? AND status = 'active'",
                (field_id,),
            ).fetchone()
            if row is None:
                raise NotFound("要删除的测量列不存在或已删除。")
            new_version = _bump_version(conn, version)
            now = _utc_now_iso()
            conn.execute(
                "UPDATE fields SET status = 'deleted', deleted_version = ?, "
                "deleted_at = ? WHERE id = ?",
                (new_version, now, field_id),
            )
            _append_field_event(
                conn,
                event_type="field_deleted",
                created_at=now,
                field_id=field_id,
                field_name=row["name"],
                structure_version=new_version,
                old_name=row["name"],
                new_name=None,
                source=source,
            )
            log.info("deleted %s at v%s", field_id, new_version)
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("删除测量列失败，结构未改变。") from exc
    return read_schema(settings)


# ---------------------------------------------------------------------------
# CSV -> Parquet
# ---------------------------------------------------------------------------


def parse_csv(data: bytes, schema: Schema) -> ParsedCsv:
    if not data:
        raise ValidationError("上传文件为空。")
    if len(data) > MAX_UPLOAD_BYTES:
        raise ValidationError(
            f"CSV 文件不能超过 {MAX_UPLOAD_BYTES // (1024 * 1024)} MiB。"
        )
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError("CSV 必须是 UTF-8 编码（可带 BOM）。") from exc

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        raw_header = next(reader)
    except StopIteration:
        raise ValidationError("CSV 缺少表头。") from None

    header = [unicodedata.normalize("NFC", h.strip()) for h in raw_header]
    if not header or all(not h for h in header):
        raise ValidationError("CSV 表头为空。")
    if any(not h for h in header):
        raise ValidationError("CSV 表头中存在空列名。")
    if len(set(header)) != len(header):
        raise ValidationError("CSV 表头中存在重复列名。")

    fixed: dict[str, int] = {}
    for required in (STATION_COLUMN, TIME_COLUMN):
        try:
            fixed[required] = header.index(required)
        except ValueError:
            raise ValidationError(
                f"CSV 必须包含固定列“{required}”。"
            ) from None
        if header.count(required) != 1:
            raise ValidationError(f"固定列“{required}”只能出现一次。")

    active = schema.active
    by_name = {f.name: f for f in active}
    selected: dict[str, Field] = {}
    unknown: list[str] = []
    for idx, name in enumerate(header):
        if idx in fixed.values():
            continue
        field = by_name.get(name)
        if field is None:
            unknown.append(name)
        else:
            selected[name] = field
    if unknown:
        available = "、".join(f.name for f in active) or "（无）"
        raise ValidationError(
            "CSV 含当前结构之外的测量列："
            + "、".join(unknown)
            + f"。当前可接收的测量列为：{available}。"
            "请刷新页面使用最新结构导出/上传。"
        )

    stations: list[str] = []
    times: list[datetime] = []
    values: dict[str, list[int | None]] = {
        field.id: [] for field in selected.values()
    }

    station_idx = header.index(STATION_COLUMN)
    time_idx = header.index(TIME_COLUMN)
    selected_by_index = {
        idx: field for idx, name in enumerate(header) if (field := selected.get(name))
    }

    row_count = 0
    for row in reader:
        if not row:
            continue
        row_count += 1
        if row_count > MAX_ROWS:
            raise ValidationError(f"每批最多 {MAX_ROWS} 行数据。")
        line_no = reader.line_num
        if len(row) != len(header):
            raise ValidationError(
                f"CSV 第 {line_no} 行列数为 {len(row)}，表头为 {len(header)} 列。"
            )

        station = row[station_idx].strip()
        if not station:
            raise ValidationError(f"CSV 第 {line_no} 行站号为空。")
        if len(station) > MAX_STATION_LENGTH:
            raise ValidationError(f"CSV 第 {line_no} 行站号过长。")
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in station):
            raise ValidationError(f"CSV 第 {line_no} 行站号含控制字符。")
        stations.append(station)

        raw_time = row[time_idx].strip()
        if not raw_time:
            raise ValidationError(f"CSV 第 {line_no} 行 UTC 时刻为空。")
        try:
            parsed_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(
                f"CSV 第 {line_no} 行时刻“{raw_time}”不是 ISO 8601 格式。"
            ) from None
        if parsed_time.tzinfo is None:
            parsed_time = parsed_time.replace(tzinfo=timezone.utc)
        else:
            parsed_time = parsed_time.astimezone(timezone.utc)
        times.append(parsed_time)

        for idx, field in selected_by_index.items():
            raw_value = row[idx].strip()
            if raw_value == "":
                parsed_value: int | None = None
            else:
                if not INT_RE.fullmatch(raw_value):
                    raise ValidationError(
                        f"CSV 第 {line_no} 行“{field.name}”的值“{raw_value}”"
                        "不是整数。空值请留空。"
                    )
                parsed_value = int(raw_value)
                if parsed_value < INT64_MIN or parsed_value > INT64_MAX:
                    raise ValidationError(
                        f"CSV 第 {line_no} 行“{field.name}”的值超出 64 位整数范围。"
                    )
            values[field.id].append(parsed_value)

    if row_count == 0:
        raise ValidationError("CSV 至少需要 1 行数据。")

    filled: dict[str, tuple[int | None, ...]] = {}
    for field in active:
        if field.id in values:
            filled[field.id] = tuple(values[field.id])
        else:
            filled[field.id] = tuple([None] * row_count)

    return ParsedCsv(
        stations=tuple(stations),
        times=tuple(times),
        values=filled,
        header_fields=tuple(selected.values()),
    )


def _build_upload_table(parsed: ParsedCsv, schema: Schema) -> pa.Table:
    names = [STATION_COLUMN, TIME_COLUMN]
    arrays: list[pa.Array] = [
        pa.array(parsed.stations, type=pa.string()),
        pa.array(parsed.times, type=pa.timestamp("us", tz="UTC")),
    ]
    for field in schema.active:
        names.append(field.id)
        arrays.append(pa.array(parsed.values[field.id], type=pa.int64()))
    return pa.Table.from_arrays(arrays, names=names)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_path(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    try:
        _fsync_path(path)
    except OSError as exc:  # 某些文件系统不支持目录 fsync
        log.warning("目录 fsync 失败 %s: %s", path, exc)


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        log.exception("清理文件失败 %s", path)


def _cleanup_stale_temps(settings: Settings) -> None:
    """清理超过 1 小时的临时文件。

    新写入的临时文件命名以点号开头（``.xxx.tmp``），这里的 glob 覆盖两种形态；
    用“足够旧”阈值避免误删另一实例正在写入的临时文件。
    """

    cutoff = time.time() - 3600
    try:
        candidates = [
            p
            for p in settings.parquet_dir.iterdir()
            if p.suffix == ".tmp" and p.is_file() and p.stat().st_mtime < cutoff
        ]
    except OSError:
        log.warning("扫描临时文件失败")
        return
    for path in candidates:
        try:
            path.unlink(missing_ok=True)
            log.warning("清理陈旧临时文件：%s", path.name)
        except OSError:
            log.warning("无法清理临时文件 %s", path)


def _cleanup_orphan_files(settings: Settings) -> None:
    """清理崩溃遗留的、未被任何批次登记的 Parquet。

    只处理超过 1 小时的文件，避免误删另一实例正在转换 / 即将登记的新文件。
    """

    cutoff = time.time() - 3600
    try:
        candidates = [
            p
            for p in settings.parquet_dir.glob("*.parquet")
            if p.is_file() and p.stat().st_mtime < cutoff
        ]
    except OSError:
        log.warning("扫描孤立 Parquet 失败")
        return
    if not candidates:
        return
    try:
        registered = {b.file_name for b in read_state(settings).batches}
    except LakeError:
        return
    for path in candidates:
        if path.name in registered:
            continue
        try:
            # 再确认一次：另一实例可能刚登记该文件
            if path.name in {b.file_name for b in read_state(settings).batches}:
                continue
            path.unlink(missing_ok=True)
            log.warning("清理未登记的孤立 Parquet：%s", path.name)
        except (OSError, LakeError):
            log.warning("无法清理孤立 Parquet：%s", path.name)


def _write_parquet_atomic(
    table: pa.Table, final_path: Path, metadata: dict[bytes, bytes]
) -> str:
    """临时文件 -> fsync -> rename -> fsync 目录；返回内容 sha256。"""

    table = table.replace_schema_metadata(metadata)
    tmp_path = final_path.with_name(f".{final_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        pq.write_table(table, tmp_path, compression="snappy")
        _fsync_path(tmp_path)
        digest = _sha256_file(tmp_path)
        os.replace(tmp_path, final_path)
        _fsync_dir(final_path.parent)
        return digest
    except BaseException:
        _safe_unlink(tmp_path)
        raise


# ---------------------------------------------------------------------------
# 批次上传与读取
# ---------------------------------------------------------------------------


def upload_csv(
    settings: Settings,
    expected_version: int,
    data: bytes,
    *,
    source: str | None = None,
) -> Batch:
    """解析并转换后，在最终提交前再次校验结构版本。

    转换/写文件期间不持有数据库写锁，因此结构仍可被另一进程修改；提交时
    若版本已变化，则批次绝不登记，刚写的 Parquet 也会被清理。
    """

    schema = read_schema(settings)
    if schema.version != expected_version:
        raise StaleSchema(expected_version, schema.version)

    parsed = parse_csv(data, schema)
    table = _build_upload_table(parsed, schema)

    batch_id = uuid.uuid4().hex
    created_at = _utc_now_iso()
    file_name = f"{batch_id}.parquet"
    final_path = settings.parquet_dir / file_name
    fields_at_upload = [
        {"id": f.id, "name": f.name, "ordinal": f.ordinal} for f in schema.active
    ]
    schema_json = json.dumps(fields_at_upload, ensure_ascii=False, separators=(",", ":"))
    metadata = {
        UPLOAD_METADATA_PREFIX + b"format": b"1",
        UPLOAD_METADATA_PREFIX + b"batch_id": batch_id.encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"schema_version": str(schema.version).encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"created_at": created_at.encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"fields": schema_json.encode("utf-8"),
    }

    try:
        file_sha256 = _write_parquet_atomic(table, final_path, metadata)
    except OSError as exc:
        raise StorageError("写入 Parquet 失败，批次未登记。") from exc

    try:
        if _before_commit_hook is not None:
            _before_commit_hook()
        batch = _commit_batch(
            settings=settings,
            expected_version=expected_version,
            batch_id=batch_id,
            created_at=created_at,
            row_count=len(parsed.stations),
            file_name=file_name,
            file_sha256=file_sha256,
            schema_json=schema_json,
            fields_at_upload=fields_at_upload,
            source=source,
        )
    except BaseException:
        _safe_unlink(final_path)
        raise
    log.info(
        "batch %s committed rows=%s schema=v%s", batch_id, len(parsed.stations), schema.version
    )
    return batch


def _commit_batch(
    *,
    settings: Settings,
    expected_version: int,
    batch_id: str,
    created_at: str,
    row_count: int,
    file_name: str,
    file_sha256: str,
    schema_json: str,
    fields_at_upload: Sequence[dict],
    source: str | None = None,
) -> Batch:
    try:
        with _write_transaction(settings) as conn:
            _check_expected_version(conn, expected_version)
            conn.execute(
                "INSERT INTO batches(id, created_at, schema_version, row_count, "
                "file_name, file_sha256, schema_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    created_at,
                    expected_version,
                    row_count,
                    file_name,
                    file_sha256,
                    schema_json,
                ),
            )
            _append_batch_event(
                conn,
                created_at=created_at,
                schema_version=expected_version,
                batch_id=batch_id,
                row_count=row_count,
                file_name=file_name,
                schema_json=schema_json,
                source=source,
            )
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("批次登记失败，未写入新批次。") from exc
    return Batch(
        id=batch_id,
        created_at=created_at,
        schema_version=expected_version,
        row_count=row_count,
        file_name=file_name,
        file_sha256=file_sha256,
        fields_at_upload=tuple(fields_at_upload),
    )


def get_batch(settings: Settings, batch_id: str) -> BatchView:
    """读取批次；结构快照与批次登记来自同一个已提交读事务。"""

    try:
        with _read_transaction(settings) as conn:
            schema = _query_schema(conn)
            row = conn.execute(
                "SELECT id, created_at, schema_version, row_count, file_name, "
                "file_sha256, schema_json FROM batches WHERE id = ?",
                (batch_id,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise StorageError("读取批次失败。") from exc

    if row is None:
        raise NotFound("批次不存在。")
    batch = _rows_to_batch(row)
    physical = _read_physical_table(settings, batch)
    current = _materialize_current_table(physical, schema)
    return BatchView(schema=schema, batch=batch, table=current)


def read_original_batch(settings: Settings, batch_id: str) -> tuple[Batch, bytes]:
    """返回批次登记与**原始上传**的 Parquet 字节（物理列名 field_id，不重写）。"""

    try:
        with _read_transaction(settings) as conn:
            row = conn.execute(
                "SELECT id, created_at, schema_version, row_count, file_name, "
                "file_sha256, schema_json FROM batches WHERE id = ?",
                (batch_id,),
            ).fetchone()
    except sqlite3.Error as exc:
        raise StorageError("读取批次失败。") from exc
    if row is None:
        raise NotFound("批次不存在。")
    batch = _rows_to_batch(row)
    path = settings.parquet_dir / batch.file_name
    if not path.is_file():
        raise BatchIntegrityError(
            f"批次 {batch.id[:8]} 的原始 Parquet 文件缺失：{batch.file_name}"
        )
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise BatchIntegrityError(
            f"批次 {batch.id[:8]} 的原始 Parquet 无法读取。"
        ) from exc
    return batch, payload


def _read_physical_table(settings: Settings, batch: Batch) -> pa.Table:
    path = settings.parquet_dir / batch.file_name
    if not path.is_file():
        raise BatchIntegrityError(
            f"批次 {batch.id[:8]} 的 Parquet 文件缺失：{batch.file_name}"
        )
    try:
        table = pq.read_table(path)
    except (OSError, pa.ArrowException) as exc:
        raise BatchIntegrityError(f"批次 {batch.id[:8]} 的 Parquet 无法读取。") from exc
    for fixed in (STATION_COLUMN, TIME_COLUMN):
        if fixed not in table.column_names:
            raise BatchIntegrityError(f"批次 {batch.id[:8]} 缺少固定列 {fixed}。")
    return table


def _materialize_current_table(physical: pa.Table, schema: Schema) -> pa.Table:
    """按当前已提交结构生成只读视图；不重写历史文件。

    当前字段 id 在历史文件中不存在（新字段，或删除后同名重建）时补 NULL。
    """

    row_count = physical.num_rows
    names = [STATION_COLUMN, TIME_COLUMN]
    arrays: list[pa.Array] = [
        physical.column(STATION_COLUMN).combine_chunks().cast(pa.string()),
        physical.column(TIME_COLUMN).combine_chunks().cast(
            pa.timestamp("us", tz="UTC")
        ),
    ]
    for field in schema.active:
        names.append(field.name)
        if field.id in physical.column_names:
            try:
                column = physical.column(field.id).combine_chunks().cast(pa.int64())
            except (pa.ArrowException, KeyError) as exc:
                raise BatchIntegrityError(
                    f"历史 Parquet 的列 {field.id} 类型异常。"
                ) from exc
        else:
            column = pa.nulls(row_count, type=pa.int64())
        arrays.append(column)
    return pa.Table.from_arrays(arrays, names=names)


def csv_bytes(table: pa.Table) -> bytes:
    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(table.column_names)
    for record in table.to_pylist():
        row: list[str] = []
        for name in table.column_names:
            value = record[name]
            if value is None:
                row.append("")
            elif isinstance(value, datetime):
                row.append(format_utc(value))
            else:
                row.append(str(value))
        writer.writerow(row)
    return out.getvalue().encode("utf-8")


def parquet_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue().to_pybytes()


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def display_cell(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return format_utc(value)
    return str(value)


def rows_as_display(table: pa.Table) -> tuple[list[str], list[list[str | None]]]:
    columns = list(table.column_names)
    rows = [
        [display_cell(record[name]) for name in columns]
        for record in table.to_pylist()
    ]
    return columns, rows


def sample_csv(schema: Schema, rows: int = 5) -> bytes:
    """按当前结构生成可直接上传的示例 CSV，用于网页演示。"""

    stations = ["54511", "54512", "58362", "50953", "45007"]
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    active = schema.active
    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow([STATION_COLUMN, TIME_COLUMN, *[f.name for f in active]])
    for i in range(rows):
        row = [
            stations[i % len(stations)],
            format_utc(base.replace(hour=i % 24, minute=(i * 7) % 60)),
        ]
        for j, _field in enumerate(active):
            # 留一个空值，演示可空整数列
            row.append("" if (i + j) % 4 == 3 else str(10 + (i * 3 + j * 5) % 40))
        writer.writerow(row)
    return out.getvalue().encode("utf-8")


def check_event_integrity(settings: Settings) -> tuple[int, ...]:
    """核对订阅启用后的结构版本 / 字段 / 批次都有对应事件。

    返回缺失的结构版本列表；订阅启用前已存在的历史不参与核对。
    """

    try:
        with _read_transaction(settings) as conn:
            schema = _query_schema(conn)
            start_row = conn.execute(
                "SELECT value FROM meta WHERE key = ?", (EVENT_JOURNAL_START_KEY,)
            ).fetchone()
            start_version = int(start_row["value"]) if start_row is not None else 0
            start_at_row = conn.execute(
                "SELECT value FROM meta WHERE key = ?",
                (EVENT_JOURNAL_START_AT_KEY,),
            ).fetchone()
            start_at = start_at_row["value"] if start_at_row is not None else ""
            batch_rows = conn.execute(
                "SELECT id, schema_version, created_at FROM batches"
            ).fetchall()
            event_rows = conn.execute(
                "SELECT type, structure_version, field_id, batch_id FROM events"
            ).fetchall()
    except sqlite3.Error as exc:
        raise StorageError("核对变更事件失败。") from exc

    structure_versions = {
        int(r["structure_version"])
        for r in event_rows
        if r["structure_version"] is not None
    }
    added_ids = {
        r["field_id"]
        for r in event_rows
        if r["type"] == "field_added" and r["field_id"] is not None
    }
    deleted_versions = {
        (r["field_id"], int(r["structure_version"]))
        for r in event_rows
        if r["type"] == "field_deleted" and r["field_id"] is not None
    }
    event_batch_ids = {r["batch_id"] for r in event_rows if r["batch_id"] is not None}

    missing: list[int] = [
        v
        for v in range(start_version + 1, schema.version + 1)
        if v not in structure_versions
    ]
    for field in schema.fields:
        if field.created_version > start_version and field.id not in added_ids:
            missing.append(field.created_version)
        if (
            field.deleted_version is not None
            and field.deleted_version > start_version
            and (field.id, field.deleted_version) not in deleted_versions
        ):
            missing.append(field.deleted_version)
    for row in batch_rows:
        # 订阅启用前入湖的批次不补写、也不核对；启用后入湖（版本更高或
        # 入湖时间晚于起点）则必须有对应事件。
        if int(row["schema_version"]) <= start_version and row["created_at"] <= start_at:
            continue
        if row["id"] not in event_batch_ids:
            missing.append(int(row["schema_version"]))
    return tuple(sorted(set(missing)))


def check_integrity(settings: Settings) -> IntegrityReport:
    state = read_state(settings)
    registered = {b.file_name for b in state.batches}
    missing: list[str] = []
    mismatched: list[str] = []
    event_gaps = check_event_integrity(settings)
    try:
        disk_files = {
            p.name for p in settings.parquet_dir.glob("*.parquet") if p.is_file()
        }
        temp_files = sorted(
            p.name
            for p in settings.parquet_dir.iterdir()
            if p.suffix == ".tmp" and p.is_file()
        )
    except OSError as exc:
        raise StorageError("扫描 Parquet 目录失败。") from exc

    for batch in state.batches:
        path = settings.parquet_dir / batch.file_name
        if not path.is_file():
            missing.append(batch.file_name)
            continue
        try:
            if _sha256_file(path) != batch.file_sha256:
                mismatched.append(batch.file_name)
        except OSError:
            missing.append(batch.file_name)

    # 正在转换、尚未登记的新文件是正常瞬时状态；只把 60 秒以上的未登记文件
    # 视为孤立文件，避免并发上传时误报。
    orphan_cutoff = time.time() - 60
    orphans: list[str] = []
    for name in sorted(disk_files - registered):
        try:
            if (settings.parquet_dir / name).stat().st_mtime < orphan_cutoff:
                orphans.append(name)
        except OSError:
            continue
    return IntegrityReport(
        ok=not missing and not mismatched and not event_gaps,
        batches=len(state.batches),
        missing_files=tuple(missing),
        hash_mismatches=tuple(mismatched),
        orphan_files=tuple(orphans),
        temp_files=tuple(temp_files),
        event_gaps=event_gaps,
    )
