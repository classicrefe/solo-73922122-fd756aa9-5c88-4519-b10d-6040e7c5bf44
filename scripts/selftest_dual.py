"""端到端自测：起两个真实进程 + 共享卷，走完整演示流程。

限制：本机沙箱内无法向其他进程发信号，因此实例通过 METLAKE_STOP_FILE
优雅退出（脚本自身控制，Docker Compose 路径不依赖该机制）。

运行：python scripts/selftest_dual.py
"""

from __future__ import annotations

import csv
import io
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_health(url: str, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{url}/healthz", timeout=1.0)
            if resp.status_code == 200:
                return resp.json()
        except httpx.HTTPError:
            pass
        time.sleep(0.15)
    raise SystemExit(f"实例未就绪：{url}")


def csv_bytes(header: list[str], rows: list[list[str]]) -> bytes:
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def cell(view: dict, row: int, column: str) -> object:
    return view["rows"][row][view["columns"].index(column)]


def check(label: str, condition: bool, detail: object = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {label}" + (f"  -> {detail}" if detail != "" else ""))
    if not condition:
        raise SystemExit(1)


def main() -> None:
    data_dir = Path(tempfile.mkdtemp(prefix="metlake-selftest-"))
    ports = [free_port(), free_port()]
    procs: list[subprocess.Popen] = []
    stops: list[Path] = []
    urls = [f"http://127.0.0.1:{p}" for p in ports]
    for idx, port in enumerate(ports):
        stop = data_dir / f"stop-{idx}.flag"
        stops.append(stop)
        env = {
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "DATA_DIR": str(data_dir),
            "INSTANCE_NAME": f"node{idx + 1}",
            "METLAKE_STOP_FILE": str(stop),
        }
        procs.append(
            subprocess.Popen(
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
            )
        )

    try:
        healths = [wait_health(u) for u in urls]
        print(f"数据目录：{data_dir}")
        for h in healths:
            print(f"实例 {h['instance']} pid={h['pid']} schema=v{h['schema_version']}")
        check("两个实例是不同进程", healths[0]["pid"] != healths[1]["pid"])
        A, B = urls

        # 1. 建结构
        for name in ("temp_c", "humidity"):
            state = httpx.get(f"{A}/api/state").json()
            v = state["schema"]["schema_version"]
            r = httpx.post(f"{B}/api/fields", json={"schema_version": v, "name": name})
            check(f"在 B 新增列 {name}（版本 v{v}）", r.status_code == 201, r.text[:120])
        state = httpx.get(f"{A}/api/state").json()
        check("A 立即看到 B 提交的结构 v2", state["schema"]["schema_version"] == 2)

        # 2. 上传首批
        old = httpx.post(
            f"{A}/api/batches",
            data={"version": 2},
            files={"file": ("old.csv", csv_bytes(
                ["station_id", "observed_at", "temp_c", "humidity"],
                [["54511", "2026-01-01T00:00:00Z", "7", "40"],
                 ["54512", "2026-01-01T01:00:00Z", "9", ""]],
            ), "text/csv")},
            timeout=30,
        )
        check("A 上传 v2 批次", old.status_code == 201, old.text[:160])
        old = old.json()
        old_path = data_dir / "parquet" / old["file_name"]
        before_bytes = old_path.read_bytes()
        before_mtime = old_path.stat().st_mtime_ns

        # 3. 并发结构修改：同一旧版本只允许一个成功
        results: list[httpx.Response] = []

        def attempt(url: str) -> None:
            results.append(
                httpx.post(
                    f"{url}/api/fields",
                    json={"schema_version": 2, "name": "pressure_hpa"},
                    timeout=30,
                )
            )

        threads = [threading.Thread(target=attempt, args=(A,)), threading.Thread(target=attempt, args=(B,))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        codes = sorted(r.status_code for r in results)
        check("并发提交同版本：一成功一被拒", codes == [201, 409], codes)

        # 4. 改名，旧批次沿用旧值
        fields = {f["name"]: f["id"] for f in httpx.get(f"{A}/api/schema").json()["fields"]}
        v = httpx.get(f"{A}/api/schema").json()["schema_version"]
        r = httpx.patch(f"{B}/api/fields/{fields['temp_c']}", json={"schema_version": v, "name": "temperature_c"})
        check("改名 temp_c -> temperature_c", r.status_code == 200, r.text[:120])
        view = httpx.get(f"{A}/api/batches/{old['id']}").json()
        check("旧批次按新列名展示且仍为旧值",
              view["columns"][2] == "temperature_c" and cell(view, 0, "temperature_c") == "7",
              view["columns"])
        check("历史 Parquet 未被改写",
              old_path.read_bytes() == before_bytes and old_path.stat().st_mtime_ns == before_mtime)

        # 5. 删除后同名新增：不串值
        v = httpx.get(f"{A}/api/schema").json()["schema_version"]
        r = httpx.request("DELETE", f"{A}/api/fields/{fields['temp_c']}", json={"schema_version": v})
        check("删除测量列", r.status_code == 200, r.text[:120])
        v = httpx.get(f"{B}/api/schema").json()["schema_version"]
        r = httpx.post(f"{A}/api/fields", json={"schema_version": v, "name": "temp_c"})
        check("同名新增得到新字段", r.status_code == 201)
        new_id = r.json()["fields"][-1]["id"]
        check("新字段 id 与旧字段不同", new_id != fields["temp_c"])
        v = httpx.get(f"{A}/api/schema").json()["schema_version"]
        new = httpx.post(
            f"{B}/api/batches",
            data={"version": v},
            files={"file": ("new.csv", csv_bytes(
                ["station_id", "observed_at", "temp_c", "humidity"],
                [["54511", "2026-02-01T00:00:00Z", "88", "55"]],
            ), "text/csv")},
            timeout=30,
        )
        check("B 上传新结构批次", new.status_code == 201, new.text[:160])
        old_view = httpx.get(f"{A}/api/batches/{old['id']}").json()
        check("已删除列不再出现在展示中", "temperature_c" not in old_view["columns"], old_view["columns"])
        check("旧批次同名新列为空（未串值）", cell(old_view, 0, "temp_c") is None, old_view["rows"][0])
        new_view = httpx.get(f"{A}/api/batches/{new.json()['id']}").json()
        check("新批次读到新值", cell(new_view, 0, "temp_c") == "88")
        check("新批次其他列正确", cell(new_view, 0, "humidity") == "55")

        # 6. 过期版本上传被整次拒绝且不落文件
        v = httpx.get(f"{A}/api/schema").json()["schema_version"]
        stale = httpx.post(
            f"{B}/api/batches",
            data={"version": v - 1},
            files={"file": ("stale.csv", csv_bytes(
                ["station_id", "observed_at", "temp_c"], [["1", "2026-03-01T00:00:00Z", "1"]]
            ), "text/csv")},
            timeout=30,
        )
        check("旧结构上传被拒绝", stale.status_code == 409, stale.text[:120])
        files = list((data_dir / "parquet").glob("*.parquet"))
        check("拒绝的上传未留下 Parquet", len(files) == 2, [p.name for p in files])

        # 7. 下载与完整性
        csv_dl = httpx.get(f"{A}/api/batches/{new.json()['id']}?format=csv")
        expected_header = httpx.get(f"{A}/api/schema").json()
        names = ["station_id", "observed_at"] + [f["name"] for f in expected_header["fields"]]
        check("下载按当前列名 CSV", csv_dl.status_code == 200 and csv_dl.text.splitlines()[0] == ",".join(names),
              csv_dl.text.splitlines()[0])
        pq_dl = httpx.get(f"{A}/api/batches/{new.json()['id']}?format=parquet")
        check("下载当前列名 Parquet", pq_dl.status_code == 200 and len(pq_dl.content) > 100)
        integrity = httpx.get(f"{A}/api/integrity").json()
        check("双实例一致性自检", integrity["ok"] and integrity["batches"] == 2, integrity)
        page = httpx.get(f"{B}/")
        check("网页可打开展示过期提示", page.status_code == 200)
        print("\n全部端到端检查通过。")
    finally:
        for path in stops:
            path.touch()
        for proc in procs:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    main()
