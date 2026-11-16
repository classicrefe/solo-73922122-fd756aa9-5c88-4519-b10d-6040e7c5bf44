# 气象观测数据湖

FastAPI + PyArrow(Parquet) + SQLite 的观测数据湖示例。网页维护一张观测表：

- 固定列：`station_id`（站号）、`observed_at`（UTC 时刻，写入前统一换算为 UTC）；
- 最多 8 个可空 `int64` 测量列，可新增 / 改名 / 删除；
- 每批上传采集站导出的 CSV（≤1000 行），转换为 Parquet 入湖；
- 可按**当前**列名查看、下载任一批次（CSV / Parquet）；
- 按批清退：填写原因后受理，永久拒绝该批新查看与 CSV / Parquet 下载，
  原件在所有进行中的读取结束后物理删除，并保留批号、原因、状态供回查。

## 设计要点（对应需求）

| 需求 | 实现 |
| --- | --- |
| 改名沿用旧值 | Parquet 的物理列名是永久 `field_id`（`f_<uuid4>`），列名只存在于 `fields` 表；改名只更新 `fields.name`，历史文件不动 |
| 删除后同名新增不串值 | 删除是软删除（`status='deleted'`，保留 id）；同名新增生成新的 `field_id`；历史批次中没有新 id 的列在读取时补 `NULL` |
| 历史 Parquet 不得重写 | 只做“读 + 内存投影”，从不以写方式打开历史文件；新文件以临时文件写入后 rename |
| 结构版本 | SQLite `meta.schema_version` 单调递增；每次新增/改名/删除提交时 +1 |
| 过期整次拒绝 | 表单/API 必须携带页面所见版本；写事务内 `BEGIN IMMEDIATE` 先比对版本，不等则整次拒绝（HTTP 409 / 网页提示） |
| 转换期间仍可改结构 | 上传路径只在解析前后短暂读库，写 Parquet 时不持锁；提交登记时二次比对版本，若结构已变则拒绝登记并删除刚写的文件 |
| 双进程并发 | SQLite WAL + `busy_timeout` + `BEGIN IMMEDIATE` 串行化写入；两实例共享数据目录 |
| 失败/重启后一致 | 批次登记与结构变更同一事务；文件先落盘再登记，登记失败删文件；启动时清理陈旧临时文件与（超过 1 小时的）孤儿 Parquet |
| 查询只用同一已提交结构 | `get_batch` / 页面在单个读事务中读取结构与批次登记；不读未提交数据 |
| 失败保留已有批次 | 任何失败路径都不改已有登记与文件；`/api/integrity` 可核对登记与文件 sha256 |
| 受理即拒绝新读取 | 清退只做一个短写事务，往 `retirements` 表登记后立即返回；`get_batch` 在准入阶段读到登记即返回 HTTP 410（页面 / CSV / Parquet / JSON 全覆盖），慢读取不会拖住清退受理 |
| 已获准读取返回完整原结果 | 读取采用两段式准入：先查无清退登记，再取该批共享读锁（`flock LOCK_SH`）并复查；持锁期间回收无法删文件，读取即使长时间暂停也能拿到完整原件 |
| 双实例活读者保护 | 回收对每批用非阻塞排他锁（`flock LOCK_EX|LOCK_NB`），与共享读锁互斥；被占用就跳过本批，绝不等待。`flock` 由内核在句柄关闭或进程退出时自动释放，随后自动回收 |
| 文件实际清除才算完成 | 只有 `unlink` 成功（并 fsync 目录、确认文件不存在）才把状态置为 `retired`；删除失败置 `cleanup_failed` 并保留原因，下一轮（含重启后）自动续清 |
| 重启不遗忘、不复供 | 清退状态全部持久化在共享 SQLite；两实例后台回收线程启动即扫描 `pending_cleanup` / `cleanup_failed`，已登记批次在任何重启后仍返回 410 |
| 重复清退复用原状态 | 同一批次再次清退直接返回原登记，不覆盖管理员填写的原因；全程不 chmod / chown，目录权限保持管理员设置 |
| 批次间互不阻塞 | 回收按批独立加锁、独立处理；一批读取或删除停顿不影响其他批次的上传、取数与清退 |
| 清退回查 | `retirements` 表保留批号、原因、状态、受理实例 / 时间、尝试次数与最近错误，网页 `/retirements` 与 `/api/retirements` 均可回查 |

### 清退状态机

```
受理（填写原因）
      │  INSERT retirements(status='pending_cleanup')
      ▼
pending_cleanup ──回收拿到排他锁且 unlink 成功──▶ retired（已清退，读 410）
      │                                              ▲
      └──unlink 失败──▶ cleanup_failed ──下一轮/重启后自动重试──┘
```

三种状态下新读取一律 410；只有 `retired` 表示 Parquet 原件已确认从共享卷删除。
清退不改写也不删除 `batches` 登记本身（批号、行数、sha256 等历史身份保留），
仅物理删除对应的 Parquet 数据文件。

## 本机运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
DATA_DIR=./data INSTANCE_NAME=node-a uvicorn app.main:app --port 8000
```

另一个终端用相同 `DATA_DIR`、`INSTANCE_NAME=node-b` 起第二个实例（端口 8001），
分别打开网页即可演示双进程共享结构与批次。

## Docker Compose（双实例 + 共享持久卷）

```bash
docker compose up --build -d
# 浏览器打开 http://localhost:8001 (node-a) 与 http://localhost:8002 (node-b)
docker compose down          # 保留命名卷 lake-data
docker compose down -v       # 连同数据一起删除
```

两个服务共用命名卷 `lake-data`，`DATA_DIR=/data`。

## 网页演示流程

1. `http://localhost:8001/` 新增测量列 `temp_c`、`humidity`；
2. 下载“示例 CSV”，或自行准备：

   ```csv
   station_id,observed_at,temp_c,humidity
   54511,2026-01-01T00:00:00Z,7,40
   54512,2026-01-01T01:00:00Z,9,
   ```

3. 上传后进入批次页查看；
4. 在 `node-b` 把 `temp_c` 改名为 `temperature_c`，回到 `node-a` 刷新批次页：列名新、值仍为旧值；
5. 删除 `temp_c`（或已改名的列）后新增同名 `temp_c`，再上传新批次：
   旧批次该列显示 `NULL`，新批次显示新值；旧 Parquet 文件字节未变；
6. 打开两个标签页（或分别用两个实例）同时提交基于同一版本的结构修改：
   一个成功、另一个提示“结构版本已过期”并被整次拒绝；
7. `/api/integrity` 或首页“一致性自检”核对登记与文件一致。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/state` | 结构快照 + 批次列表 |
| GET | `/healthz` | 实例名、pid、版本 |
| POST | `/api/fields` | `{"schema_version": v, "name": "..."}` |
| PATCH | `/api/fields/{field_id}` | 改名，body 同上 |
| DELETE | `/api/fields/{field_id}` | 软删除，body `{"schema_version": v}` |
| POST | `/api/batches` | multipart：`version` + `file` |
| GET | `/api/batches/{id}?format=json\|csv\|parquet` | 按当前结构读取/下载；已清退返回 410 |
| POST | `/api/batches/{id}/retire` | `{"reason": "..."}` 受理清退；重复受理复用原登记 |
| GET | `/api/retirements` | 清退登记列表（批号 / 原因 / 状态 / 错误 / 尝试次数） |
| GET | `/retirements` | 清退回查网页（含在役批次的清退表单） |
| GET | `/api/integrity` | 文件与登记一致性自检（已清退缺文件不计异常） |

过期版本返回 409 `{"error": "stale_schema", ...}`；参数错误 400；文件缺失 500 `integrity_error`；
批次已清退返回 410 `{"error": "batch_retired", ...}`。

网页清退入口：首页批次列表与批次详情页均有“清退”表单（原因必填，最长 500 字），
提交后跳转到 `/retirements` 回查页。

## 测试

```bash
python -m pytest -q                 # 43 项：核心/网页/一致性/清退；含真实 uvicorn 进程的并发与跨进程锁测试
python scripts/selftest_dual.py     # 端到端演示脚本：起两个真实进程 + 共享目录，逐项打印 PASS/FAIL（含清退全流程）
```

## 清退演示流程

1. 首页批次列表（或批次详情页）填写**清退原因**并提交；
2. 页面提示已受理，该批的查看 / CSV / Parquet 立即返回“批次已清退”（410），
   即使 Parquet 尚未物理删除也不再放行新读取；
3. 打开 `/retirements` 回查：状态先为“待清理”，回收线程确认删除后变“已清退”；
4. 若此时另一实例正持有慢读取，状态停留在“待清理”且原件保留；读取结束或读者
   进程退出（内核自动释放 `flock`）后自动变为“已清退”；
5. 把共享卷的 `parquet` 目录临时改为只读再清退，可看到“清理失败（待自动重试）”
   与保留的错误原因；恢复权限后无需重新受理即自动续清完成；
6. 对同一批再次清退，原因与状态保持首次受理结果，登记不会新增；
7. 清退期间其他批次的上传、改名、取数与清退均不受影响。

## 已知限制

- 单机 SQLite + 共享卷方案适用于演示与中小规模；跨主机需换成服务端数据库与对象存储。
- 网页为服务端渲染，无前端路由；并发冲突通过版本号拒绝，不做自动合并。
- 删除只做逻辑删除，Parquet 中历史字段的物理列会一直保留（这是“不重写历史”的代价）。
- 活读者保护依赖 POSIX `flock`，要求共享卷是本地盘或支持 `flock` 语义的文件系统
  （ext4 / xfs / overlayfs / 多数命名卷均可）；个别网络文件系统（老 NFS、某些 SMB
  挂载）锁语义不完整，部署前应在目标卷上验证。
- 清退回收是每实例一个后台线程，按 `METLAKE_REAP_INTERVAL`（默认 1 秒）轮询；
  受理时会唤醒本实例线程，其他实例最迟一个轮询周期内跟进。
- 沙箱环境无 `docker`，Compose 配置与镜像构建未在本机实际运行；已用两个真实
  uvicorn 进程共享目录（含第三个独立慢读者进程）完成同等并发与锁语义验证。
