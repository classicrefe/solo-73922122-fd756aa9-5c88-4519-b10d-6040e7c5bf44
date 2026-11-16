"""启动一个实例：python scripts/serve.py [--port 8000]

除了常规 uvicorn 启动外，支持可选的 METLAKE_STOP_FILE：该文件出现时优雅退出。
用于自测脚本在受限环境中停止后台实例，生产 Compose 不设置该变量。
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from pathlib import Path

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import create_app  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args()

    config = uvicorn.Config(
        create_app(),
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        access_log=False,
    )
    server = uvicorn.Server(config)

    stop_file = os.environ.get("METLAKE_STOP_FILE")
    if stop_file:
        stop_path = Path(stop_file)

        def watch() -> None:
            while not stop_path.exists():
                time.sleep(0.2)
            server.should_exit = True

        threading.Thread(target=watch, daemon=True).start()

    server.run()


if __name__ == "__main__":
    main()
