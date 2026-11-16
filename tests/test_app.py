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

    monkeypatch.setattr(lake.pq, "write_table", boom)
    try:
        failed = client.post(
            "/api/batches",
            data={"version": v},
            files={"file": ("b.csv", simple_csv(temp_c=2), "text/csv")},
        )
    finally:
        monkeypatch.setattr(lake.pq, "write_table", pq.write_table)
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
