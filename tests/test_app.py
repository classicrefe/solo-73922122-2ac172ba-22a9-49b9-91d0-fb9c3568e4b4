"""核心与 Web 层测试。运行：python -m pytest -q"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from pyarrow import parquet as pq

from app import lake
from app.lake import Settings
from app.main import create_app

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    s = Settings(data_dir=tmp_path / "lake", instance_name="test")
    lake.initialize(s)
    return s


@pytest.fixture()
def client(settings: Settings):
    with TestClient(create_app(settings)) as c:
        yield c


def csv_text(header: list[str], rows: list[list[str]]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def simple_csv(station="54511", time="2026-01-01T00:00:00Z", **values) -> bytes:
    header = [lake.STATION_COLUMN, lake.TIME_COLUMN, *values.keys()]
    row = [station, time, *(str(v) if v is not None else "" for v in values.values())]
    return csv_text(header, [row])


def current(client: TestClient) -> dict:
    resp = client.get("/api/state")
    assert resp.status_code == 200
    return resp.json()


def add(client: TestClient, name: str, version: int | None = None) -> httpx.Response:
    if version is None:
        version = current(client)["schema"]["schema_version"]
    return client.post(
        "/api/fields", json={"schema_version": version, "name": name}
    )


def file_digest(path: Path) -> tuple[str, int]:
    return hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns


# ---------------------------------------------------------------------------
# 结构：新增 / 改名 / 删除
# ---------------------------------------------------------------------------


def test_add_field_and_version(client: TestClient):
    assert current(client)["schema"]["schema_version"] == 0
    resp = add(client, "temp_c")
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["schema_version"] == 1
    assert [f["name"] for f in body["fields"]] == ["temp_c"]
    health = client.get("/healthz").json()
    assert health["schema_version"] == 1


def test_add_field_stale_version_rejected(client: TestClient):
    add(client, "temp_c")
    stale = add(client, "humidity", version=0)
    assert stale.status_code == 409
    payload = stale.json()
    assert payload["error"] == "stale_schema"
    assert payload["schema_version"] == 1
    assert payload["expected_version"] == 0
    state = current(client)
    assert state["schema"]["schema_version"] == 1
    assert [f["name"] for f in state["schema"]["fields"]] == ["temp_c"]


def test_rename_keeps_values_and_does_not_rewrite_file(client: TestClient, settings: Settings):
    add(client, "temp_c")
    version = current(client)["schema"]["schema_version"]
    up = client.post(
        "/api/batches",
        data={"version": version},
        files={"file": ("a.csv", simple_csv(temp_c=21), "text/csv")},
    )
    assert up.status_code == 201, up.text
    batch = up.json()
    path = settings.parquet_dir / batch["file_name"]
    before = file_digest(path)

    field_id = current(client)["schema"]["fields"][0]["id"]
    version = current(client)["schema"]["schema_version"]
    renamed = client.patch(
        f"/api/fields/{field_id}",
        json={"schema_version": version, "name": "temperature_c"},
    )
    assert renamed.status_code == 200, renamed.text
    assert renamed.json()["schema_version"] == version + 1
    assert file_digest(path) == before

    view = client.get(f"/api/batches/{batch['id']}").json()
    assert view["columns"] == ["station_id", "observed_at", "temperature_c"]
    assert view["rows"][0][2] == "21"
    assert view["batch"]["fields_at_upload"][0]["name"] == "temp_c"

    csv_resp = client.get(f"/api/batches/{batch['id']}?format=csv")
    assert csv_resp.status_code == 200
    assert csv_resp.text.splitlines()[0] == "station_id,observed_at,temperature_c"
    assert ",21" in csv_resp.text


def test_delete_then_readd_same_name_does_not_leak_values(client: TestClient, settings: Settings):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    old = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("old.csv", simple_csv(temp_c=7), "text/csv")},
    ).json()
    old_path = settings.parquet_dir / old["file_name"]
    old_digest = file_digest(old_path)
    old_field = current(client)["schema"]["fields"][0]

    v = current(client)["schema"]["schema_version"]
    assert client.request(
        "DELETE",
        f"/api/fields/{old_field['id']}",
        json={"schema_version": v},
    ).status_code == 200
    assert current(client)["schema"]["fields"] == []
    assert current(client)["schema"]["deleted_fields"][0]["name"] == "temp_c"

    v = current(client)["schema"]["schema_version"]
    readd = client.post("/api/fields", json={"schema_version": v, "name": "temp_c"})
    assert readd.status_code == 201, readd.text
    new_field = readd.json()["fields"][0]
    assert new_field["id"] != old_field["id"]

    v = current(client)["schema"]["schema_version"]
    new = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("new.csv", simple_csv(temp_c=99), "text/csv")},
    ).json()

    old_view = client.get(f"/api/batches/{old['id']}").json()
    assert old_view["columns"][2:] == ["temp_c"]
    assert old_view["rows"][0][2] is None, "旧批次不得串到同名新字段"

    new_view = client.get(f"/api/batches/{new['id']}").json()
    assert new_view["rows"][0][2] == "99"

    assert file_digest(old_path) == old_digest
    assert pq.read_table(old_path).column(old_field["id"]).to_pylist() == [7]


def test_rename_to_existing_or_reserved_name_rejected(client: TestClient):
    add(client, "temp_c")
    add(client, "humidity")
    v = current(client)["schema"]["schema_version"]
    fields = {f["name"]: f["id"] for f in current(client)["schema"]["fields"]}
    dup = client.patch(
        f"/api/fields/{fields['humidity']}",
        json={"schema_version": v, "name": "TEMP_C"},
    )
    assert dup.status_code == 400
    reserved = client.patch(
        f"/api/fields/{fields['humidity']}",
        json={"schema_version": v, "name": "observed_at"},
    )
    assert reserved.status_code == 400
    assert current(client)["schema"]["schema_version"] == v


def test_max_eight_fields(client: TestClient):
    for i in range(8):
        assert add(client, f"m{i}").status_code == 201
    v = current(client)["schema"]["schema_version"]
    ninth = add(client, "m8")
    assert ninth.status_code == 400
    state = current(client)
    assert state["schema"]["schema_version"] == v
    assert len(state["schema"]["fields"]) == 8


def test_web_forms_use_hidden_version_and_reject_stale(client: TestClient):
    page = client.get("/")
    assert page.status_code == 200
    assert 'name="version" value="0"' in page.text
    ok = client.post("/fields/add", data={"version": 0, "name": "temp_c"}, follow_redirects=False)
    assert ok.status_code == 303
    assert "v1" in ok.headers["location"]

    stale = client.post(
        "/fields/add", data={"version": 0, "name": "humidity"}, follow_redirects=False
    )
    assert stale.status_code == 303
    assert "kind=error" in stale.headers["location"]
    assert current(client)["schema"]["schema_version"] == 1

    redirected = client.post(
        "/fields/add", data={"version": 0, "name": "humidity"}, follow_redirects=True
    )
    assert "结构版本已过期" in redirected.text


# ---------------------------------------------------------------------------
# 上传解析
# ---------------------------------------------------------------------------


def test_upload_subset_of_columns_fills_nulls(client: TestClient):
    add(client, "temp_c")
    add(client, "humidity")
    v = current(client)["schema"]["schema_version"]
    resp = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=3), "text/csv")},
    )
    assert resp.status_code == 201, resp.text
    view = client.get(f"/api/batches/{resp.json()['id']}").json()
    assert view["columns"] == ["station_id", "observed_at", "temp_c", "humidity"]
    assert view["rows"][0] == ["54511", "2026-01-01T00:00:00Z", "3", None]


def test_upload_stale_version_rejected_and_no_file(client: TestClient, settings: Settings):
    add(client, "temp_c")
    stale = client.post(
        "/api/batches",
        data={"version": 0},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
    )
    assert stale.status_code == 409
    assert list(settings.parquet_dir.glob("*.parquet")) == []
    assert current(client)["batches"] == []


def test_upload_unknown_column_rejected(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    bad = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(wind=5), "text/csv")},
    )
    assert bad.status_code == 400
    assert "wind" in bad.json()["detail"]


@pytest.mark.parametrize(
    "payload, fragment",
    [
        (b"station_id,observed_at\n", "至少需要 1 行"),
        (b"station_id,observed_at\n54511,not-a-time\n", "ISO 8601"),
        (b"station_id,observed_at\n,2026-01-01T00:00:00Z\n", "站号为空"),
        (b"station_id\n54511\n", "observed_at"),
        (b"station_id,observed_at,station_id\n54511,2026-01-01T00:00:00Z,1\n", "重复列名"),
        (b"", "为空"),
        (b"\xff\xfe", "UTF-8"),
    ],
)
def test_bad_csv_rejected(client: TestClient, payload: bytes, fragment: str):
    v = current(client)["schema"]["schema_version"]
    resp = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("bad.csv", payload, "text/csv")},
    )
    assert resp.status_code == 400, resp.text
    assert fragment in resp.json()["detail"]
    assert current(client)["batches"] == []


def test_bad_integer_and_too_many_rows(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    bad_int = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c="1.5"), "text/csv")},
    )
    assert bad_int.status_code == 400
    assert "不是整数" in bad_int.json()["detail"]

    rows = [["54511", "2026-01-01T00:00:00Z", str(i)] for i in range(1001)]
    too_many = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("b.csv", csv_text(["station_id", "observed_at", "temp_c"], rows), "text/csv")},
    )
    assert too_many.status_code == 400
    assert "1000" in too_many.json()["detail"]

    rows = [["54511", "2026-01-01T00:00:00Z", str(i)] for i in range(1000)]
    ok = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("c.csv", csv_text(["station_id", "observed_at", "temp_c"], rows), "text/csv")},
    )
    assert ok.status_code == 201, ok.text
    assert ok.json()["row_count"] == 1000


def test_upload_during_conversion_with_changed_schema_is_rejected(
    client: TestClient, settings: Settings, monkeypatch
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    add(client, "humidity")  # v2
    add(client, "temp_c_extra")  # v3
    # 回到 v3，用 v3 上传
    v = current(client)["schema"]["schema_version"]

    target = current(client)["schema"]["fields"][0]["id"]

    def change_structure() -> None:
        # 模拟另一实例在“转换文件期间”提交改名（直接走核心层，避开 TestClient 线程限制）
        lake.rename_field(settings, v, target, "temp_air")

    monkeypatch.setattr(lake, "_before_commit_hook", change_structure)
    try:
        resp = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("a.csv", simple_csv(temp_c=5), "text/csv")},
        )
    finally:
        monkeypatch.setattr(lake, "_before_commit_hook", None)
    assert resp.status_code == 409
    state = current(client)
    assert state["batches"] == []
    assert list(settings.parquet_dir.glob("*.parquet")) == [], "被拒绝的旧结构上传不得留下文件"
    assert state["schema"]["fields"][0]["name"] == "temp_air"


def test_write_failure_keeps_existing_batches(client: TestClient, settings: Settings, monkeypatch):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    good = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
    )
    assert good.status_code == 201
    before = current(client)

    def boom(*args, **kwargs):
        raise OSError("disk full")

    real_write = pq.write_table
    monkeypatch.setattr(lake.pq, "write_table", boom)
    try:
        failed = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("b.csv", simple_csv(temp_c=2), "text/csv")},
        )
    finally:
        monkeypatch.setattr(lake.pq, "write_table", real_write)
    assert failed.status_code == 500
    after = current(client)
    assert after["batches"] == before["batches"]
    assert len(list(settings.parquet_dir.glob("*.parquet"))) == 1
    assert client.get(f"/api/batches/{good.json()['id']}").status_code == 200


def test_registration_failure_removes_new_file(client: TestClient, settings: Settings, monkeypatch):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]

    def fail(*args, **kwargs):
        raise lake.StorageError("登记失败（模拟）")

    monkeypatch.setattr(lake, "_commit_batch", fail)
    try:
        resp = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
        )
    finally:
        monkeypatch.setattr(lake, "_commit_batch", lake._commit_batch)
    assert resp.status_code == 500
    assert list(settings.parquet_dir.glob("*.parquet")) == []
    assert current(client)["batches"] == []


# ---------------------------------------------------------------------------
# 一致性 / 重启 / 完整性
# ---------------------------------------------------------------------------


def test_restart_preserves_schema_batches_files(settings: Settings):
    app = create_app(settings)
    with TestClient(app) as c:
        add(c, "temp_c")
        v = current(c)["schema"]["schema_version"]
        batch = c.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("a.csv", simple_csv(temp_c=8), "text/csv")},
        ).json()
        schema_before = current(c)["schema"]

    restarted = TestClient(create_app(Settings(data_dir=settings.data_dir, instance_name="again")))
    with restarted:
        state = current(restarted)
        assert state["schema"] == schema_before
        assert [b["id"] for b in state["batches"]] == [batch["id"]]
        view = restarted.get(f"/api/batches/{batch['id']}").json()
        assert view["rows"][0][2] == "8"
        assert restarted.get("/api/integrity").json()["ok"] is True


def test_reader_sees_only_committed_schema(settings: Settings):
    schema = lake.add_field(settings, 0, "temp_c")
    field_id = schema.fields[0].id
    version = schema.version

    blocker = sqlite3.connect(settings.db_path, isolation_level=None)
    blocker.execute("PRAGMA busy_timeout = 5000")
    blocker.execute("BEGIN IMMEDIATE")
    blocker.execute("UPDATE fields SET name = 'uncommitted_name' WHERE id = ?", (field_id,))
    blocker.execute("UPDATE meta SET value = ? WHERE key = 'schema_version'", (str(version + 1),))
    try:
        seen = lake.read_schema(settings)
        assert seen.version == version
        assert seen.fields[0].name == "temp_c"
        state = lake.read_state(settings)
        assert state.schema.version == version
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()

    assert lake.read_schema(settings).fields[0].name == "temp_c"


def test_batch_view_is_single_snapshot_under_concurrent_renames(settings: Settings):
    lake.add_field(settings, 0, "temp_c")
    schema = lake.read_schema(settings)
    batch = lake.upload_csv(settings, schema.version, simple_csv(temp_c=42))
    field_id = schema.fields[0].id

    stop = threading.Event()
    errors: list[BaseException] = []

    def flipper() -> None:
        current_schema = lake.read_schema(settings)
        current_name = "temp_c"
        while not stop.is_set():
            try:
                new_name = "temp_b" if current_name == "temp_c" else "temp_c"
                current_schema = lake.rename_field(
                    settings, current_schema.version, field_id, new_name
                )
                current_name = new_name
            except BaseException as exc:  # pragma: no cover
                errors.append(exc)
                return

    thread = threading.Thread(target=flipper, daemon=True)
    thread.start()
    try:
        for _ in range(80):
            view = lake.get_batch(settings, batch.id)
            names = list(view.table.column_names)
            assert names == [lake.STATION_COLUMN, lake.TIME_COLUMN] + [
                f.name for f in view.schema.active
            ]
            assert view.table.column(names[2]).to_pylist() == [42]
            # 结构快照与列名必须来自同一版本，不能出现混合列名
            assert view.schema.active[0].name in ("temp_b", "temp_c")
    finally:
        stop.set()
        thread.join(timeout=5)
    assert errors == []


def test_integrity_detects_missing_file_and_read_fails_cleanly(
    client: TestClient, settings: Settings
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]

    def upload(name: str, value: int) -> dict:
        resp = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": (name, simple_csv(temp_c=value), "text/csv")},
        )
        assert resp.status_code == 201, resp.text
        return resp.json()

    first = upload("a.csv", 1)
    second = upload("b.csv", 2)
    assert client.get("/api/integrity").json()["ok"] is True

    (settings.parquet_dir / first["file_name"]).unlink()
    report = client.get("/api/integrity").json()
    assert report["ok"] is False
    assert report["missing_files"] == [first["file_name"]]

    broken = client.get(f"/api/batches/{first['id']}")
    assert broken.status_code == 500
    assert "缺失" in broken.json()["detail"]

    still_ok = client.get(f"/api/batches/{second['id']}")
    assert still_ok.status_code == 200
    assert still_ok.json()["rows"][0][2] == "2"


def test_timestamp_normalised_to_utc(client: TestClient):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    payload = csv_text(
        ["station_id", "observed_at", "temp_c"],
        [["54511", "2026-01-01T08:30:00+08:00", "12"]],
    )
    batch = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", payload, "text/csv")},
    ).json()
    view = client.get(f"/api/batches/{batch['id']}").json()
    assert view["rows"][0][1] == "2026-01-01T00:30:00Z"


def test_batch_not_found(client: TestClient):
    assert client.get("/api/batches/nope").status_code == 404
    page = client.get("/batches/nope")
    assert page.status_code == 404
    assert "未找到" in page.text


# ---------------------------------------------------------------------------
# 真实双进程（uvicorn x2，共享数据目录）
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_health(url: str, timeout: float = 25.0) -> dict:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{url}/healthz", timeout=1.0)
            if resp.status_code == 200:
                return resp.json()
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.15)
    raise AssertionError(f"实例未就绪 {url}: {last}")


@pytest.fixture()
def dual_instances(tmp_path: Path):
    data_dir = tmp_path / "shared"
    ports = [_free_port(), _free_port()]
    urls: list[str] = []
    procs: list[subprocess.Popen] = []
    stop_files: list[Path] = []
    for idx, port in enumerate(ports):
        stop_file = tmp_path / f"stop-{idx}.flag"
        stop_files.append(stop_file)
        env = {
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "DATA_DIR": str(data_dir),
            "INSTANCE_NAME": f"node{idx + 1}",
            "METLAKE_STOP_FILE": str(stop_file),
        }
        proc = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "scripts" / "serve.py"),
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        procs.append(proc)
        urls.append(f"http://127.0.0.1:{port}")
    try:
        for url in urls:
            _wait_health(url)
        yield urls, data_dir
    finally:
        for path in stop_files:
            path.touch()
        for proc in procs:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()


def test_two_processes_share_state_and_reject_stale_writes(dual_instances):
    urls, data_dir = dual_instances
    a, b = urls

    health_a = httpx.get(f"{a}/healthz").json()
    health_b = httpx.get(f"{b}/healthz").json()
    assert health_a["pid"] != health_b["pid"]
    assert health_a["instance"] == "node1" and health_b["instance"] == "node2"

    created = httpx.post(f"{a}/api/fields", json={"schema_version": 0, "name": "temp_c"})
    assert created.status_code == 201, created.text
    assert httpx.get(f"{b}/api/schema").json()["schema_version"] == 1

    results: list[httpx.Response] = []

    def attempt(url: str) -> None:
        results.append(
            httpx.post(
                f"{url}/api/fields",
                json={"schema_version": 1, "name": "humidity"},
                timeout=30,
            )
        )

    left = threading.Thread(target=attempt, args=(a,))
    right = threading.Thread(target=attempt, args=(b,))
    left.start()
    right.start()
    left.join()
    right.join()
    codes = sorted(r.status_code for r in results)
    assert codes == [201, 409], [r.text for r in results]

    state_a = httpx.get(f"{a}/api/state").json()
    state_b = httpx.get(f"{b}/api/state").json()
    assert state_a["schema"] == state_b["schema"]
    assert state_a["schema"]["schema_version"] == 2
    assert {f["name"] for f in state_a["schema"]["fields"]} == {"temp_c", "humidity"}

    # 在 B 上传，A 立即可见，且一致性自检通过
    upload = httpx.post(
        f"{b}/api/batches",
        data={"version": 2},
        files={"file": ("a.csv", simple_csv(temp_c=5, humidity=40), "text/csv")},
        timeout=30,
    )
    assert upload.status_code == 201, upload.text
    batch_id = upload.json()["id"]
    view_a = httpx.get(f"{a}/api/batches/{batch_id}").json()
    assert view_a["rows"][0] == ["54511", "2026-01-01T00:00:00Z", "5", "40"]
    integrity = httpx.get(f"{a}/api/integrity").json()
    assert integrity["ok"] is True and integrity["batches"] == 1

    # 用过期版本在 A 上传，B 中不得出现新批次或文件
    stale = httpx.post(
        f"{a}/api/batches",
        data={"version": 1},
        files={"file": ("b.csv", simple_csv(temp_c=6, humidity=41), "text/csv")},
        timeout=30,
    )
    assert stale.status_code == 409
    assert len(httpx.get(f"{b}/api/state").json()["batches"]) == 1
    assert len(list((data_dir / "parquet").glob("*.parquet"))) == 1

    # 两个真实进程看到同一份只追加事件；过期上传没有留下事件
    events_a = httpx.get(f"{a}/api/events?cursor=0&limit=100").json()
    events_b = httpx.get(f"{b}/api/events?cursor=0&limit=100").json()
    assert events_a["events"] == events_b["events"]
    assert [e["type"] for e in events_a["events"]] == [
        "field_added",
        "field_added",
        "batch_registered",
    ]
    assert events_a["events"][-1]["batch_id"] == batch_id
    assert events_a["next_cursor"] == "3"

    # 在 A 翻页中途由 B 提交，再用游标从 A 续读：无空洞、无重复
    page1 = httpx.get(f"{a}/api/events?cursor=0&limit=2").json()
    assert [e["id"] for e in page1["events"]] == [1, 2]
    created = httpx.post(
        f"{b}/api/fields",
        json={"schema_version": 2, "name": "wind_ms"},
    )
    assert created.status_code == 201, created.text
    rest = httpx.get(f"{a}/api/events?cursor={page1['next_cursor']}&limit=100").json()
    assert [e["id"] for e in rest["events"]] == [3, 4]
    assert rest["events"][0]["batch_id"] == batch_id
    assert rest["events"][1]["type"] == "field_added"
    retry = httpx.get(f"{a}/api/events?cursor={page1['next_cursor']}&limit=100").json()
    assert retry["events"] == rest["events"]

    # 原始下载在 B 上同样可用且字节一致
    original = httpx.get(f"{b}/api/batches/{batch_id}/original")
    assert original.status_code == 200
    assert original.content == (data_dir / "parquet" / f"{batch_id}.parquet").read_bytes()


# ---------------------------------------------------------------------------
# 变更事件订阅：永久 ID、游标分页、原子性与续读
# ---------------------------------------------------------------------------


def event_ids(resp: httpx.Response) -> list[int]:
    assert resp.status_code == 200, resp.text
    return [e["id"] for e in resp.json()["events"]]


def page_through(client: TestClient, limit: int = 2) -> tuple[list[dict], list[str]]:
    """按 next_cursor 一页页读到 has_more=False，返回全部事件与每页游标。"""

    events: list[dict] = []
    cursors: list[str] = []
    cursor = ""
    while True:
        resp = client.get("/api/events", params={"cursor": cursor, "limit": limit})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert len(body["events"]) <= limit
        events.extend(body["events"])
        cursors.append(body["next_cursor"])
        if not body["has_more"]:
            return events, cursors
        cursor = body["next_cursor"]


def test_event_journal_records_all_changes_in_commit_order(client: TestClient):
    add(client, "temp_c")  # event 1, v1
    add(client, "humidity")  # event 2, v2
    field_id = current(client)["schema"]["fields"][0]["id"]
    v = current(client)["schema"]["schema_version"]
    assert client.patch(
        f"/api/fields/{field_id}", json={"schema_version": v, "name": "temperature_c"}
    ).status_code == 200  # event 3, v3
    up = client.post(
        "/api/batches",
        data={"version": 3},
        files={"file": ("a.csv", simple_csv(temperature_c=5), "text/csv")},
    )
    assert up.status_code == 201, up.text
    v = current(client)["schema"]["schema_version"]
    assert client.request(
        "DELETE", f"/api/fields/{field_id}", json={"schema_version": v}
    ).status_code == 200  # event 5, v4

    events, cursors = page_through(client, limit=2)
    assert [e["type"] for e in events] == [
        "field_added",
        "field_added",
        "field_renamed",
        "batch_registered",
        "field_deleted",
    ]
    assert [e["id"] for e in events] == [1, 2, 3, 4, 5]
    assert cursors == ["2", "4", "5"]

    assert events[0]["new_name"] == "temp_c" and events[0]["structure_version"] == 1
    renamed = events[2]
    assert renamed["field_id"] == field_id
    assert renamed["old_name"] == "temp_c"
    assert renamed["new_name"] == "temperature_c"
    assert renamed["structure_version"] == 3

    deleted = events[4]
    assert deleted["field_id"] == field_id
    assert deleted["old_name"] == "temperature_c"
    assert deleted["new_name"] is None
    assert deleted["structure_version"] == 4

    batch_event = events[3]
    assert batch_event["batch_id"] == up.json()["id"]
    assert batch_event["batch_row_count"] == 1
    assert batch_event["schema_version"] == 3
    # 批次事件带上入湖时字段的永久 ID 与名称
    upload_view = client.get(f"/api/batches/{up.json()['id']}").json()
    assert batch_event["batch_fields"] == upload_view["batch"]["fields_at_upload"]
    assert [f["name"] for f in batch_event["batch_fields"]] == [
        "temperature_c",
        "humidity",
    ]
    assert [f["ordinal"] for f in batch_event["batch_fields"]] == [0, 1]
    assert batch_event["original_parquet_url"] == f"/api/batches/{up.json()['id']}/original"


def test_event_ids_are_permanent_across_rename_delete_and_readd(client: TestClient):
    add(client, "temp_c")
    old_id = current(client)["schema"]["fields"][0]["id"]
    v = current(client)["schema"]["schema_version"]
    client.patch(
        f"/api/fields/{old_id}", json={"schema_version": v, "name": "temp_air"}
    )
    v = current(client)["schema"]["schema_version"]
    client.request("DELETE", f"/api/fields/{old_id}", json={"schema_version": v})
    v = current(client)["schema"]["schema_version"]
    readd = client.post("/api/fields", json={"schema_version": v, "name": "temp_c"})
    assert readd.status_code == 201, readd.text
    new_id = readd.json()["fields"][0]["id"]
    assert new_id != old_id

    events, _ = page_through(client, limit=10)
    first = events[0]
    renamed = events[1]
    deleted = events[2]
    readded = events[3]

    # 旧事件一律不变
    assert first["field_id"] == old_id
    assert first["new_name"] == "temp_c"
    assert renamed["old_name"] == "temp_c" and renamed["new_name"] == "temp_air"
    assert deleted["field_id"] == old_id and deleted["old_name"] == "temp_air"
    assert deleted["new_name"] is None
    # 同名重建是新事件、新 field_id，不能改写旧事件
    assert readded["field_id"] == new_id and readded["new_name"] == "temp_c"
    assert all(e["id"] < readded["id"] for e in (first, renamed, deleted))


def test_event_cursor_pagination_is_gapless_under_concurrent_commits(
    settings: Settings,
):
    """翻页期间有新提交：既不漏事件，也不重复、不改写已返回事件。"""

    for name in ("a", "b", "c"):
        lake.add_field(settings, lake.read_schema(settings).version, name)
    seen: list[int] = []
    cursor = ""
    pages = 0
    while True:
        page = lake.read_events(settings, cursor=cursor, limit=1)
        pages += 1
        seen.extend(e.id for e in page.events)
        cursor = page.next_cursor
        # 每翻一页后，另一“实例”提交一条新结构变更
        if pages <= 3:
            lake.add_field(
                settings, lake.read_schema(settings).version, f"late{pages}"
            )
        if not page.has_more:
            break
    # 最终应看到全部 3 + 3 条事件，且严格递增无重复
    all_ids = [e.id for e in lake.read_events(settings, limit=200).events]
    assert seen == all_ids
    assert len(seen) == 6
    assert seen == sorted(set(seen))

    # 用旧游标重试：返回相同的后续事件，不会跳过
    retry_from_third = lake.read_events(settings, cursor=str(seen[2]), limit=2)
    assert [e.id for e in retry_from_third.events] == seen[3:5]
    # 重复请求同一页完全幂等
    again = lake.read_events(settings, cursor=str(seen[2]), limit=2)
    assert again.events == retry_from_third.events


def test_event_cursor_rejects_invalid_values(client: TestClient):
    add(client, "temp_c")
    for bad in ("-1", "abc", "1.5", "1e3"):
        resp = client.get("/api/events", params={"cursor": bad})
        assert resp.status_code == 400, (bad, resp.text)
        assert resp.json()["error"] == "invalid_cursor"
    for bad in ("0", "-2", "x", str(999)):
        resp = client.get("/api/events", params={"limit": bad})
        assert resp.status_code == 400, (bad, resp.text)


def test_event_page_is_single_snapshot_while_writer_commits(
    settings: Settings,
):
    """读事务进行中提交的新事件不会混入本页或造成空洞。"""

    for name in ("a", "b", "c", "d"):
        lake.add_field(settings, lake.read_schema(settings).version, name)

    page = lake.read_events(settings, cursor="0", limit=3)
    assert [e.id for e in page.events] == [1, 2, 3]
    assert page.has_more is True
    assert page.next_cursor == "3"

    # 页边界之后的新提交只能出现在后续页
    lake.add_field(settings, lake.read_schema(settings).version, "e")
    rest = lake.read_events(settings, cursor=page.next_cursor, limit=10)
    assert [e.id for e in rest.events] == [4, 5]
    assert rest.has_more is False


def test_event_and_batch_registration_are_atomic(
    client: TestClient, settings: Settings, monkeypatch
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    before = lake.read_events(settings, limit=100).events
    assert [e.type for e in before] == ["field_added"]

    def fail(*args, **kwargs):
        raise lake.StorageError("登记失败（模拟）")

    monkeypatch.setattr(lake, "_commit_batch", fail)
    try:
        resp = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
        )
    finally:
        monkeypatch.setattr(lake, "_commit_batch", lake._commit_batch)
    assert resp.status_code == 500
    assert [e.type for e in lake.read_events(settings, limit=100).events] == [
        "field_added"
    ]
    assert list(settings.parquet_dir.glob("*.parquet")) == []


def test_event_and_schema_change_are_atomic_on_failure(
    client: TestClient, settings: Settings, monkeypatch
):
    add(client, "temp_c")
    field_id = current(client)["schema"]["fields"][0]["id"]
    v = current(client)["schema"]["schema_version"]

    def boom(*args, **kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    real_append = lake._append_field_event
    monkeypatch.setattr(lake, "_append_field_event", boom)
    try:
        resp = client.patch(
            f"/api/fields/{field_id}",
            json={"schema_version": v, "name": "temperature_c"},
        )
    finally:
        monkeypatch.setattr(lake, "_append_field_event", real_append)
    assert resp.status_code == 500
    # 结构版本、字段名、事件都必须一起回滚
    assert current(client)["schema"]["schema_version"] == v
    assert current(client)["schema"]["fields"][0]["name"] == "temp_c"
    assert [e.type for e in lake.read_events(settings, limit=100).events] == [
        "field_added"
    ]


def test_event_subscription_pages_and_original_download(client: TestClient):
    page = client.get("/changes")
    assert page.status_code == 200
    assert "变更订阅" in page.text
    assert "next_cursor" in page.text

    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    batch = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=9), "text/csv")},
    ).json()

    page2 = client.get("/changes?cursor=0&limit=1")
    assert page2.status_code == 200
    assert 'value="0"' in page2.text
    # 有下一页时页面给出续读链接
    assert "cursor=1" in page2.text

    # 整页渲染包含批次事件：显示入湖字段名、批次链接与原始下载入口
    full = client.get("/changes?cursor=0&limit=100")
    assert full.status_code == 200
    assert "temp_c" in full.text
    assert batch["id"][:8] in full.text
    assert f"/api/batches/{batch['id']}/original" in full.text
    assert "下一页" not in full.text  # 已无更多事件

    empty = client.get("/changes?cursor=999")
    assert empty.status_code == 200
    assert "没有更多事件" in empty.text

    detail = client.get("/events/2")
    assert detail.status_code == 200
    assert batch["id"] in detail.text
    assert f"/api/batches/{batch['id']}/original" in detail.text

    original = client.get(f"/api/batches/{batch['id']}/original")
    assert original.status_code == 200
    assert original.content == (
        client.app.state.settings.parquet_dir / batch["file_name"]
    ).read_bytes()
    # 原始文件物理列名是永久 field_id
    import pyarrow.parquet as pq
    import io as _io

    table = pq.read_table(_io.BytesIO(original.content))
    assert lake.STATION_COLUMN in table.column_names
    assert "temp_c" not in table.column_names
    assert any(name.startswith("f_") for name in table.column_names)

    # 按当前列名下载的行为保持不变
    current_dl = client.get(f"/api/batches/{batch['id']}?format=parquet")
    assert current_dl.status_code == 200
    current_table = pq.read_table(_io.BytesIO(current_dl.content))
    assert "temp_c" in current_table.column_names

    assert client.get("/events/999").status_code == 404
    assert client.get("/api/events/999").status_code == 404


def test_original_download_unchanged_after_rename_and_delete(
    client: TestClient, settings: Settings
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    batch = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("a.csv", simple_csv(temp_c=3), "text/csv")},
    ).json()
    before = client.get(f"/api/batches/{batch['id']}/original").content
    field_id = current(client)["schema"]["fields"][0]["id"]

    v = current(client)["schema"]["schema_version"]
    client.patch(
        f"/api/fields/{field_id}", json={"schema_version": v, "name": "temp_air"}
    )
    v = current(client)["schema"]["schema_version"]
    client.request("DELETE", f"/api/fields/{field_id}", json={"schema_version": v})

    after = client.get(f"/api/batches/{batch['id']}/original")
    assert after.status_code == 200
    assert after.content == before
    assert file_digest(settings.parquet_dir / batch["file_name"])[0] == batch["file_sha256"]

    # 事件里的字段名仍是登记时的值
    events = lake.read_events(settings, limit=100).events
    batch_event = next(e for e in events if e.type == "batch_registered")
    assert batch_event.batch_fields[0]["name"] == "temp_c"


def test_events_survive_restart(settings: Settings):
    app = create_app(settings)
    with TestClient(app) as c:
        add(c, "temp_c")
        v = current(c)["schema"]["schema_version"]
        c.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
        )
        first_page = lake.read_events(settings, limit=1)
        assert first_page.next_cursor == "1"

    restarted = TestClient(create_app(Settings(data_dir=settings.data_dir, instance_name="again")))
    with restarted:
        # 从旧游标继续读，重启后事件与游标都还在
        payload = restarted.get("/api/events?cursor=1&limit=10").json()
        assert [e["type"] for e in payload["events"]] == ["batch_registered"]
        assert restarted.get("/api/events?cursor=0&limit=10").json()["next_cursor"] == "2"
        assert restarted.get("/api/integrity").json()["event_gaps"] == []


def test_legacy_database_starts_journal_at_current_version(tmp_path: Path):
    """升级前已有结构与批次：不伪造历史事件，只在启用后记录。"""

    data_dir = tmp_path / "legacy"
    settings = Settings(data_dir=data_dir, instance_name="legacy")
    settings.ensure_dirs()
    conn = sqlite3.connect(settings.db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    conn.execute("INSERT INTO meta VALUES ('schema_version', '2')")
    conn.execute(
        "CREATE TABLE fields (id TEXT PRIMARY KEY, name TEXT NOT NULL, "
        "ordinal INTEGER NOT NULL, status TEXT NOT NULL, created_version INTEGER NOT NULL, "
        "deleted_version INTEGER, created_at TEXT NOT NULL, deleted_at TEXT)"
    )
    conn.execute(
        "INSERT INTO fields VALUES ('f_' || replace(hex(zeroblob(16)), '00', 'a'), "
        "'temp_c', 0, 'active', 1, NULL, '2026-01-01T00:00:00Z', NULL)"
    )
    conn.execute(
        "INSERT INTO fields VALUES ('f_' || replace(hex(zeroblob(16)), '00', 'b'), "
        "'wind', 1, 'deleted', 1, 2, '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z')"
    )
    conn.execute(
        "CREATE TABLE batches (id TEXT PRIMARY KEY, created_at TEXT NOT NULL, "
        "schema_version INTEGER NOT NULL, row_count INTEGER NOT NULL, "
        "file_name TEXT NOT NULL UNIQUE, file_sha256 TEXT NOT NULL, "
        "schema_json TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO batches VALUES ('legacybatch', '2026-01-03T00:00:00Z', 2, 1, "
        "'legacybatch.parquet', 'x', '[]')"
    )
    conn.close()

    lake.initialize(settings)
    page = lake.read_events(settings, limit=100)
    assert page.events == ()
    assert page.journal_start_version == 2
    assert page.next_cursor == "0"
    assert lake.check_integrity(settings).event_gaps == ()

    # 启用后的提交才开始有事件，ID 从 1 起
    legacy_field = next(f for f in lake.read_schema(settings).fields if f.name == "temp_c")
    schema = lake.rename_field(settings, 2, legacy_field.id, "temp_air")
    assert schema.version == 3
    page = lake.read_events(settings, limit=100)
    assert [(e.id, e.type, e.new_name) for e in page.events] == [
        (1, "field_renamed", "temp_air")
    ]
    assert page.journal_start_version == 2
    assert lake.check_integrity(settings).event_gaps == ()

    # 再次启动不会重写起点，也不会重复事件
    lake.initialize(settings)
    assert [e.id for e in lake.read_events(settings, limit=100).events] == [1]
    assert lake.read_events(settings, limit=1).journal_start_version == 2


def test_batch_registered_after_enable_at_same_schema_version_is_checked(tmp_path: Path):
    """启用订阅后、结构版本未变时入湖的批次也必须有事件（不能漏检）。"""

    settings = Settings(data_dir=tmp_path / "samever", instance_name="legacy")
    settings.ensure_dirs()
    conn = sqlite3.connect(settings.db_path, isolation_level=None)
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("INSERT INTO meta VALUES ('schema_version', '0')")
    conn.execute(
        "CREATE TABLE fields (id TEXT PRIMARY KEY, name TEXT NOT NULL, "
        "ordinal INTEGER NOT NULL, status TEXT NOT NULL, created_version INTEGER NOT NULL, "
        "deleted_version INTEGER, created_at TEXT NOT NULL, deleted_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE batches (id TEXT PRIMARY KEY, created_at TEXT NOT NULL, "
        "schema_version INTEGER NOT NULL, row_count INTEGER NOT NULL, "
        "file_name TEXT NOT NULL UNIQUE, file_sha256 TEXT NOT NULL, "
        "schema_json TEXT NOT NULL)"
    )
    conn.close()

    lake.initialize(settings)  # 订阅自 v0 起
    batch = lake.upload_csv(
        settings, 0, simple_csv()
    )
    assert lake.check_integrity(settings).event_gaps == ()

    # 事件在数据库层只追加：直接 UPDATE / DELETE 都会被拒绝
    conn = sqlite3.connect(settings.db_path, isolation_level=None)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE events SET new_name = 'x' WHERE batch_id = ?", (batch.id,))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM events WHERE batch_id = ?", (batch.id,))
    # 去掉触发器后人为删掉批次事件，自检必须发现
    conn.execute("DROP TRIGGER trg_events_append_only_delete")
    conn.execute("DELETE FROM events WHERE batch_id = ?", (batch.id,))
    conn.close()
    gaps = lake.check_integrity(settings).event_gaps
    assert 0 in gaps


def test_stale_upload_and_disk_failure_leave_no_event(
    client: TestClient, settings: Settings, monkeypatch
):
    add(client, "temp_c")
    v = current(client)["schema"]["schema_version"]
    baseline = [e.id for e in lake.read_events(settings, limit=100).events]

    # 过期版本：整次拒绝，连事件都不产生
    stale = client.post(
        "/api/batches",
        data={"version": v - 1},
        files={"file": ("a.csv", simple_csv(temp_c=1), "text/csv")},
    )
    assert stale.status_code == 409
    assert [e.id for e in lake.read_events(settings, limit=100).events] == baseline

    # 写盘失败：重试钩子后也不产生事件
    def boom(*args, **kwargs):
        raise OSError("disk full")

    real_write = pq.write_table
    monkeypatch.setattr(lake.pq, "write_table", boom)
    try:
        failed = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("b.csv", simple_csv(temp_c=2), "text/csv")},
        )
    finally:
        monkeypatch.setattr(lake.pq, "write_table", real_write)
    assert failed.status_code == 500
    assert [e.id for e in lake.read_events(settings, limit=100).events] == baseline
    assert list(settings.parquet_dir.glob("*.parquet")) == []

    # 随后正常上传仍是连续的下一条事件，中间没有空洞
    ok = client.post(
        "/api/batches",
        data={"version": v},
        files={"file": ("c.csv", simple_csv(temp_c=3), "text/csv")},
    )
    assert ok.status_code == 201, ok.text
    events = lake.read_events(settings, limit=100).events
    assert [e.id for e in events] == baseline + [baseline[-1] + 1]
    assert events[-1].type == "batch_registered"
    assert lake.check_integrity(settings).event_gaps == ()
