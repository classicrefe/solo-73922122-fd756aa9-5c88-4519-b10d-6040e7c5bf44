"""批次清退功能测试：受理、读锁协调、物理回收、失败续清、双进程。

运行：python -m pytest tests/test_retirement.py -q
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from app import lake
from app.lake import Settings
from app.main import create_app

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    # 测试中手动驱动回收，不依赖后台轮询时序。
    s = Settings(
        data_dir=tmp_path / "lake", instance_name="test", reap_interval=0.05
    )
    lake.initialize(s)
    return s


@pytest.fixture()
def client(settings: Settings):
    with TestClient(create_app(settings)) as c:
        yield c


@pytest.fixture()
def core_only(settings: Settings, monkeypatch):
    """不启动后台回收线程的应用：回收时机完全由测试手动驱动。"""

    class _NoopCoordinator:
        @staticmethod
        def start(s):
            return None

        @staticmethod
        def stop(s):
            return None

        @staticmethod
        def wake(s):
            return None

    monkeypatch.setattr(lake, "ReaperCoordinator", _NoopCoordinator)
    with TestClient(create_app(settings)) as c:
        yield c


def make_batch(client: TestClient, value: int = 7) -> dict:
    assert client.post(
        "/api/fields", json={"schema_version": 0, "name": "temp_c"}
    ).status_code == 201
    v = client.get("/api/state").json()["schema"]["schema_version"]
    resp = client.post(
        "/api/batches",
        data={"version": v},
        files={
            "file": (
                "a.csv",
                f"station_id,observed_at,temp_c\n54511,2026-01-01T00:00:00Z,{value}\n".encode(),
                "text/csv",
            )
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def retire(client: TestClient, batch_id: str, reason: str = "旧批次不再供分析") -> httpx.Response:
    return client.post(
        f"/api/batches/{batch_id}/retire", json={"reason": reason}
    )


def hold_read_hook(blocked_batch_id: str, entered: threading.Event, release: threading.Event):
    """只阻塞指定批次的读取，其他批次的读取立即放行。"""

    def hook(batch_id: str) -> None:
        if batch_id == blocked_batch_id:
            entered.set()
            release.wait(timeout=10)

    return hook


# ---------------------------------------------------------------------------
# 受理与读取拒绝
# ---------------------------------------------------------------------------


def test_retire_accepts_and_persists_record(client: TestClient):
    batch = make_batch(client)
    resp = retire(client, batch["id"], "项目结项，清退旧批次")
    assert resp.status_code == 201, resp.text
    record = resp.json()
    assert record["batch_id"] == batch["id"]
    assert record["reason"] == "项目结项，清退旧批次"
    assert record["status"] == lake.RETIREMENT_PENDING
    assert record["status_label"] == "待清理"
    assert record["blocks_reads"] is True
    assert record["retired"] is False
    assert record["attempts"] == 0
    assert record["requested_by"] == "test"

    listing = client.get("/api/retirements").json()["retirements"]
    assert len(listing) == 1 and listing[0]["batch_id"] == batch["id"]

    state = client.get("/api/state").json()
    assert state["batches"][0]["retirement"]["reason"] == "项目结项，清退旧批次"


def test_retire_rejects_empty_reason_and_bad_batch(client: TestClient):
    batch = make_batch(client)
    assert retire(client, batch["id"], "   ").status_code == 400
    long_reason = "x" * 501
    assert retire(client, batch["id"], long_reason).status_code == 400
    assert client.get("/api/retirements").json()["retirements"] == []

    missing = client.post(
        "/api/batches/" + ("a" * 32) + "/retire", json={"reason": "x"}
    )
    assert missing.status_code == 404
    # 非法批号也走 404，避免路径探测
    weird = client.post("/api/batches/../etc/retire", json={"reason": "x"})
    assert weird.status_code in (404, 400)


def test_new_reads_denied_after_acceptance_even_before_file_deleted(
    core_only: TestClient, settings: Settings
):
    client = core_only
    batch = make_batch(client)
    parquet = settings.parquet_dir / batch["file_name"]
    assert parquet.exists()
    assert retire(client, batch["id"]).status_code == 201

    # 尚未回收，文件还在，但所有新读取已被永久拒绝（410）
    assert parquet.exists()
    for path in (
        f"/api/batches/{batch['id']}",
        f"/api/batches/{batch['id']}?format=csv",
        f"/api/batches/{batch['id']}?format=parquet",
        f"/batches/{batch['id']}",
        f"/batches/{batch['id']}/download.csv",
        f"/batches/{batch['id']}/download.parquet",
    ):
        resp = client.get(path)
        assert resp.status_code == 410, (path, resp.status_code)
        if path.startswith("/api/"):
            assert resp.json()["error"] == "batch_retired"


def test_accepted_reader_gets_full_result_while_retirement_waits(
    core_only: TestClient, settings: Settings
):
    client = core_only
    batch = make_batch(client, value=21)
    entered = threading.Event()
    release = threading.Event()

    def slow_read(batch_id: str) -> None:
        entered.set()
        release.wait(timeout=10)

    lake.set_open_read_session_hook(slow_read)
    reader_error: list[Exception] = []

    def read_worker() -> None:
        try:
            view = lake.get_batch(settings, batch["id"])
            assert view.table.column("temp_c").to_pylist() == [21]
        except Exception as exc:  # noqa: BLE001
            reader_error.append(exc)

    thread = threading.Thread(target=read_worker)
    thread.start()
    try:
        assert entered.wait(5)
        # 读取进行中受理清退：受理必须立即完成，不等待慢读取
        started = time.time()
        resp = retire(client, batch["id"], "慢读取测试")
        assert resp.status_code == 201
        assert time.time() - started < 2.0, "清退受理被慢读取拖住"

        # 回收拿不到排他锁：文件保留，状态仍为待清理
        completed = lake.reap_pending_retirements(settings)
        assert completed == 0
        parquet = settings.parquet_dir / batch["file_name"]
        assert parquet.exists()
        record = lake.get_retirement(settings, batch["id"])
        assert record.status == lake.RETIREMENT_PENDING

        # 期间新读取已被拒绝，但活读者不受影响
        assert client.get(f"/api/batches/{batch['id']}").status_code == 410
    finally:
        release.set()
        lake.set_open_read_session_hook(None)
        thread.join(timeout=10)

    assert reader_error == []
    # 读取结束后自动回收（后台线程由 TestClient 生命周期驱动，也可手动触发）
    assert lake.reap_pending_retirements(settings) == 1
    record = lake.get_retirement(settings, batch["id"])
    assert record.status == lake.RETIREMENT_DONE
    assert not parquet.exists()
    assert record.last_error is None


def test_reaper_skips_locked_batch_but_continues_others(
    core_only: TestClient, settings: Settings
):
    """一批读取停顿不挡其他批次清退。"""

    client = core_only
    first = make_batch(client, value=1)
    # 第二个批次：复用当前结构直接上传
    v = client.get("/api/state").json()["schema"]["schema_version"]
    second = client.post(
        "/api/batches",
        data={"version": v},
        files={
            "file": (
                "b.csv",
                b"station_id,observed_at,temp_c\n54512,2026-01-02T00:00:00Z,2\n",
                "text/csv",
            )
        },
    ).json()

    # 先让第一批的慢读者进入（清退受理之前获准），再受理两批清退
    entered = threading.Event()
    release = threading.Event()
    lake.set_open_read_session_hook(hold_read_hook(first["id"], entered, release))
    held: list[None] = []

    def hold_first() -> None:
        try:
            lake.get_batch(settings, first["id"])
            held.append(None)
        except Exception:  # noqa: BLE001
            pass

    thread = threading.Thread(target=hold_first)
    thread.start()
    try:
        assert entered.wait(5)

        assert retire(client, first["id"], "批次一").status_code == 201
        assert retire(client, second["id"], "批次二").status_code == 201

        completed = lake.reap_pending_retirements(settings)
        # 第一批被锁，第二批照常完成
        assert completed == 1
        assert (settings.parquet_dir / first["file_name"]).exists()
        assert not (settings.parquet_dir / second["file_name"]).exists()
        assert lake.get_retirement(settings, second["id"]).is_retired
        assert (
            lake.get_retirement(settings, first["id"]).status
            == lake.RETIREMENT_PENDING
        )
    finally:
        release.set()
        lake.set_open_read_session_hook(None)
        thread.join(10)

    assert lake.reap_pending_retirements(settings) == 1
    assert not (settings.parquet_dir / first["file_name"]).exists()


# ---------------------------------------------------------------------------
# 删除失败 / 重启 / 重复清退
# ---------------------------------------------------------------------------


def test_delete_failure_keeps_pending_state_and_reason_then_resumes(
    core_only: TestClient, settings: Settings, monkeypatch
):
    client = core_only
    batch = make_batch(client)
    assert retire(client, batch["id"], "权限测试").status_code == 201
    parquet = settings.parquet_dir / batch["file_name"]

    real_unlink = Path.unlink

    def deny_unlink(self: Path, *args, **kwargs):
        if self == parquet:
            raise PermissionError("simulated read-only directory")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", deny_unlink)
    assert lake.reap_pending_retirements(settings) == 0
    record = lake.get_retirement(settings, batch["id"])
    assert record.status == lake.RETIREMENT_FAILED
    assert record.attempts == 1
    assert "simulated read-only directory" in (record.last_error or "")
    assert parquet.exists(), "删除失败必须保留原件与待清理状态"

    # 权限恢复后自动续清
    monkeypatch.setattr(Path, "unlink", real_unlink)
    assert lake.reap_pending_retirements(settings) == 1
    record = lake.get_retirement(settings, batch["id"])
    assert record.is_retired and record.last_error is None
    assert not parquet.exists()


def test_restart_resumes_pending_and_failed_retirements(settings: Settings):
    # 用核心层准备：一个 pending、一个 failed
    schema = lake.add_field(settings, 0, "temp_c")
    csv_data = (
        b"station_id,observed_at,temp_c\n54511,2026-01-01T00:00:00Z,1\n"
    )
    b1 = lake.upload_csv(settings, schema.version, csv_data)
    schema = lake.read_schema(settings)
    b2 = lake.upload_csv(
        settings,
        schema.version,
        b"station_id,observed_at,temp_c\n54512,2026-01-02T00:00:00Z,2\n",
    )
    lake.request_retirement(settings, b1.id, "重启测试一")
    lake.request_retirement(settings, b2.id, "重启测试二")
    # 手工把 b2 置为失败状态，模拟上次进程删除失败后退出
    with lake._write_transaction(settings) as conn:
        conn.execute(
            "UPDATE retirements SET status = ?, last_error = ?, attempts = 2 "
            "WHERE batch_id = ?",
            (lake.RETIREMENT_FAILED, "上次删除失败", b2.id),
        )
    assert (settings.parquet_dir / b1.file_name).exists()
    assert (settings.parquet_dir / b2.file_name).exists()

    # 新实例（新进程语义）启动即接管
    restarted = Settings(
        data_dir=settings.data_dir, instance_name="restarted", reap_interval=0.05
    )
    with TestClient(create_app(restarted)):
        deadline = time.time() + 10
        while time.time() < deadline:
            states = {r.batch_id: r.status for r in lake.list_retirements(settings)}
            if all(s == lake.RETIREMENT_DONE for s in states.values()):
                break
            time.sleep(0.1)
        states = {r.batch_id: r.status for r in lake.list_retirements(settings)}
        assert states == {b1.id: lake.RETIREMENT_DONE, b2.id: lake.RETIREMENT_DONE}

    assert not (settings.parquet_dir / b1.file_name).exists()
    assert not (settings.parquet_dir / b2.file_name).exists()
    # 重启后仍拒绝读取，不会复供
    with pytest.raises(lake.BatchRetired):
        lake.get_batch(settings, b1.id)


def test_repeated_retirement_reuses_original_record(
    core_only: TestClient, settings: Settings
):
    client = core_only
    batch = make_batch(client)
    first = retire(client, batch["id"], "首次原因").json()
    second = retire(client, batch["id"], "第二次原因被忽略")
    assert second.status_code == 201
    reused = second.json()
    assert reused["reason"] == "首次原因"
    assert reused["requested_at"] == first["requested_at"]
    assert reused["requested_by"] == first["requested_by"]
    assert lake.reap_pending_retirements(settings) == 1
    # 完成后再次清退仍返回同一条已完成记录
    third = retire(client, batch["id"], "完成后再试")
    assert third.status_code == 201
    assert third.json()["status"] == lake.RETIREMENT_DONE
    assert third.json()["reason"] == "首次原因"
    assert len(client.get("/api/retirements").json()["retirements"]) == 1


def test_retirement_does_not_change_directory_permissions(
    client: TestClient, settings: Settings
):
    batch = make_batch(client)
    parquet_dir = settings.parquet_dir
    lock_dir = settings.lock_dir
    before_p = parquet_dir.stat().st_mode
    before_l_parent = settings.data_dir.stat().st_mode
    assert retire(client, batch["id"], "权限保持").status_code == 201
    lake.reap_pending_retirements(settings)
    assert parquet_dir.stat().st_mode == before_p
    assert settings.data_dir.stat().st_mode == before_l_parent
    assert lock_dir.exists()


# ---------------------------------------------------------------------------
# 完整性与网页
# ---------------------------------------------------------------------------


def test_integrity_ignores_retired_missing_files_but_flags_others(
    core_only: TestClient, settings: Settings
):
    client = core_only
    retired_batch = make_batch(client, value=1)
    v = client.get("/api/state").json()["schema"]["schema_version"]
    live_batch = client.post(
        "/api/batches",
        data={"version": v},
        files={
            "file": (
                "b.csv",
                b"station_id,observed_at,temp_c\n54512,2026-01-02T00:00:00Z,2\n",
                "text/csv",
            )
        },
    ).json()
    assert retire(client, retired_batch["id"]).status_code == 201
    assert lake.reap_pending_retirements(settings) == 1
    report = client.get("/api/integrity").json()
    assert report["ok"] is True, report
    assert retired_batch["file_name"] not in report["missing_files"]

    # 在役批次文件缺失仍须报错
    (settings.parquet_dir / live_batch["file_name"]).unlink()
    report = client.get("/api/integrity").json()
    assert report["ok"] is False
    assert report["missing_files"] == [live_batch["file_name"]]


def test_retirement_web_pages_and_forms(core_only: TestClient, settings: Settings):
    client = core_only
    batch = make_batch(client)
    home = client.get("/")
    assert home.status_code == 200
    assert f"/batches/{batch['id']}/retire" in home.text
    assert 'name="reason"' in home.text

    page = client.get("/retirements")
    assert page.status_code == 200
    assert "批次清退回查" in page.text

    form = client.post(
        f"/batches/{batch['id']}/retire",
        data={"reason": "网页清退原因"},
        follow_redirects=False,
    )
    assert form.status_code == 303
    assert form.headers["location"].startswith("/retirements")
    follow = client.post(
        f"/batches/{batch['id']}/retire",
        data={"reason": "网页清退原因"},
        follow_redirects=True,
    )
    assert "网页清退原因" in follow.text
    assert "待清理" in follow.text

    lake.reap_pending_retirements(client.app.state.settings)
    done_page = client.get("/retirements")
    assert "已清退" in done_page.text
    # 首页不再给已清退批次提供查看/下载入口
    home_after = client.get("/").text
    assert f'/batches/{batch["id"]}/download.csv' not in home_after
    # 直接访问批次页得到 410 错误页
    error_page = client.get(f"/batches/{batch['id']}")
    assert error_page.status_code == 410
    assert "批次已清退" in error_page.text


def test_other_batches_upload_and_read_unaffected_during_pending(
    core_only: TestClient, settings: Settings
):
    client = core_only
    retired = make_batch(client, value=1)

    # 先让第一批的慢读者进入（清退受理之前获准）
    entered = threading.Event()
    release = threading.Event()
    lake.set_open_read_session_hook(hold_read_hook(retired["id"], entered, release))
    thread = threading.Thread(
        target=lambda: lake.get_batch(settings, retired["id"])
    )
    thread.start()
    try:
        assert entered.wait(5)
        assert retire(client, retired["id"], "清退中").status_code == 201
        # 其他批次仍可上传、读取
        v = client.get("/api/state").json()["schema"]["schema_version"]
        extra = client.post(
            "/api/batches",
            data={"version": v},
            files={
                "file": (
                    "c.csv",
                    b"station_id,observed_at,temp_c\n54599,2026-01-03T00:00:00Z,9\n",
                    "text/csv",
                )
            },
        )
        assert extra.status_code == 201, extra.text
        view = client.get(f"/api/batches/{extra.json()['id']}").json()
        assert view["rows"][0][2] == "9"
        # 结构变更也不受影响
        added = client.post(
            "/api/fields",
            json={"schema_version": v, "name": "humidity"},
        )
        assert added.status_code == 201, added.text
        # 回收循环在锁竞争下反复跳过，不抛错
        for _ in range(3):
            assert lake.reap_pending_retirements(settings) == 0
    finally:
        release.set()
        lake.set_open_read_session_hook(None)
        thread.join(10)


# ---------------------------------------------------------------------------
# 真实双进程：跨进程 flock 协调与进程退出自动回收
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
            "INSTANCE_NAME": f"rnode{idx + 1}",
            "METLAKE_STOP_FILE": str(stop_file),
            "METLAKE_REAP_INTERVAL": "0.2",
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


def _upload_one(base: str, value: int) -> dict:
    assert httpx.post(
        f"{base}/api/fields",
        json={"schema_version": 0, "name": "temp_c"},
    ).status_code in (201, 400)  # 双实例下可能已被另一实例创建
    v = httpx.get(f"{base}/api/state").json()["schema"]["schema_version"]
    resp = httpx.post(
        f"{base}/api/batches",
        data={"version": v},
        files={
            "file": (
                "a.csv",
                f"station_id,observed_at,temp_c\n54511,2026-01-01T00:00:00Z,{value}\n".encode(),
                "text/csv",
            )
        },
        timeout=30,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def test_dual_process_retirement_and_reader_exit_reclaims(dual_instances):
    (a, b), data_dir = dual_instances
    batch = _upload_one(a, 5)
    parquet = data_dir / "parquet" / batch["file_name"]
    assert parquet.exists()

    # 在 B 受理清退，A 立即拒绝新读取
    r = httpx.post(
        f"{b}/api/batches/{batch['id']}/retire",
        json={"reason": "双实例清退"},
        timeout=10,
    )
    assert r.status_code == 201, r.text
    assert httpx.get(f"{a}/api/batches/{batch['id']}").status_code == 410

    # 没有活读者：两个实例的回收线程之一会在轮询内完成物理删除
    deadline = time.time() + 15
    while time.time() < deadline:
        if not parquet.exists():
            break
        time.sleep(0.2)
    assert not parquet.exists(), "文件实际清除后才算清退完成"
    record = httpx.get(f"{a}/api/retirements").json()["retirements"][0]
    assert record["status"] == lake.RETIREMENT_DONE
    assert record["reason"] == "双实例清退"

    # 重启两个实例（模拟服务重启）：状态不遗忘、不复供
    # 通过 API 再次确认读取仍被拒绝
    assert httpx.get(f"{b}/api/batches/{batch['id']}").status_code == 410


def test_dual_process_reader_killed_then_file_reclaimed(dual_instances, tmp_path):
    (a, b), data_dir = dual_instances
    batch = _upload_one(a, 6)
    parquet = data_dir / "parquet" / batch["file_name"]

    # 起第三个独立进程作为“慢读者”：拿到共享锁后长时间暂停；
    # 通过停止文件优雅退出（等价于进程结束），验证内核自动释放 flock。
    holder_dir = tmp_path / "holder"
    holder_dir.mkdir()
    stop_file = holder_dir / "stop.flag"
    ready_file = holder_dir / "ready.flag"
    holder_script = holder_dir / "hold_read.py"
    holder_script.write_text(
        "import sys, time\n"
        "from pathlib import Path\n"
        f"sys.path.insert(0, {str(ROOT)!r})\n"
        "from app import lake\n"
        f"s = lake.Settings(data_dir=Path({str(data_dir)!r}), instance_name='holder')\n"
        "lake.initialize(s)\n"
        f"ready_file = Path({str(ready_file)!r})\n"
        f"stop_file = Path({str(stop_file)!r})\n"
        "def mark_ready(_batch_id):\n"
        "    ready_file.touch()\n"
        "    while not stop_file.exists():\n"
        "        time.sleep(0.1)\n"
        "lake.set_open_read_session_hook(mark_ready)\n"
        f"view = lake.get_batch(s, {batch['id']!r})\n"
        "assert view.table.column('temp_c').to_pylist() == [6]\n"
        "print('DONE', flush=True)\n"
    )
    holder = subprocess.Popen(
        [sys.executable, str(holder_script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    try:
        # 等待慢读者在读取会话内（已持共享锁）就绪；进程若提前崩溃则输出其日志
        deadline = time.time() + 10
        while time.time() < deadline and not ready_file.exists():
            if holder.poll() is not None:
                out = holder.stdout.read() if holder.stdout else b""
                raise AssertionError(f"慢读者进程提前退出: {out!r}")
            time.sleep(0.05)
        assert ready_file.exists(), "慢读者未能进入读取会话"

        # 活读者期间受理清退：文件不得被删
        r = httpx.post(
            f"{a}/api/batches/{batch['id']}/retire",
            json={"reason": "活读者保护"},
            timeout=10,
        )
        assert r.status_code == 201
        time.sleep(1.5)  # 等若干回收轮次
        assert parquet.exists(), "活读者暂停期间原件不能被删"
        record = httpx.get(f"{b}/api/retirements").json()["retirements"][0]
        assert record["status"] == lake.RETIREMENT_PENDING

        # 进程退出（读者结束 / 崩溃的等价场景）→ flock 自动释放 → 自动回收
        stop_file.touch()
        assert holder.wait(timeout=10) == 0
        deadline = time.time() + 15
        while time.time() < deadline:
            if not parquet.exists():
                break
            time.sleep(0.2)
        assert not parquet.exists(), "读者进程退出后应自动回收原件"
        record = httpx.get(f"{a}/api/retirements").json()["retirements"][0]
        assert record["status"] == lake.RETIREMENT_DONE
    finally:
        if holder.poll() is None:
            stop_file.touch()
            try:
                holder.wait(timeout=10)
            except subprocess.TimeoutExpired:
                holder.terminate()
                holder.wait(timeout=10)


def test_dual_process_delete_failure_resumes_after_permission_returns(
    dual_instances,
):
    (a, b), data_dir = dual_instances
    batch = _upload_one(b, 8)
    parquet = data_dir / "parquet" / batch["file_name"]

    # 让 parquet 目录只读（容器外本地进程；两个服务实例以同一用户运行）
    os.chmod(data_dir / "parquet", 0o500)
    try:
        r = httpx.post(
            f"{a}/api/batches/{batch['id']}/retire",
            json={"reason": "只读卷故障"},
            timeout=10,
        )
        assert r.status_code == 201
        deadline = time.time() + 6
        saw_failed = False
        while time.time() < deadline:
            record = httpx.get(f"{b}/api/retirements").json()["retirements"][0]
            if record["status"] == lake.RETIREMENT_FAILED:
                saw_failed = True
                assert record["last_error"]
                break
            time.sleep(0.2)
        assert saw_failed, "删除失败须保留待清理状态和原因"
        assert parquet.exists()
    finally:
        os.chmod(data_dir / "parquet", 0o755)

    # 权限恢复：无需重新受理，后台自动续清至完成
    deadline = time.time() + 15
    while time.time() < deadline:
        record = httpx.get(f"{a}/api/retirements").json()["retirements"][0]
        if record["status"] == lake.RETIREMENT_DONE:
            break
        time.sleep(0.2)
    assert record["status"] == lake.RETIREMENT_DONE
    assert not parquet.exists()
