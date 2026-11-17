# 气象观测数据湖

FastAPI + PyArrow(Parquet) + SQLite 的观测数据湖示例。网页维护一张观测表：

- 固定列：`station_id`（站号）、`observed_at`（UTC 时刻，写入前统一换算为 UTC）；
- 最多 8 个可空 `int64` 测量列，可新增 / 改名 / 删除；
- 每批上传采集站导出的 CSV（≤1000 行），转换为 Parquet 入湖；
- 可按**当前**列名查看、下载任一批次（CSV / Parquet）。

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
| 变更订阅与游标续读 | `events` 表只追加，ID 单调递增即游标；事件与字段变更/批次登记在同一事务提交；分页在单个读事务内取“连续已提交事件 + 末条游标” |
| 事件不可变 | SQLite 触发器禁止 UPDATE/DELETE；改名、删除、同名重建都产生新事件，旧事件的字段 ID、前后名称、结构版本与原件纹丝不动 |
| 原始批次下载 | 批次事件带入湖时字段 ID/名称，`/api/batches/{id}/original` 原样下载入湖时的 Parquet（物理列名 field_id，不重写）；按当前列名的下载行为不变 |
| 订阅边界 | 订阅只从启用时点开始记录；升级前已有数据不补写，避免伪造无法还原的改名历史（`journal_start_version` 可见） |

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
7. 打开 `http://localhost:8001/changes` 查看变更订阅：填入游标即可续读，
   翻页期间可在另一个实例继续提交，事件只会追加、不会漏；
8. `/api/integrity` 或首页“一致性自检”核对登记与文件一致。

## 变更订阅

`/changes` 是网页版订阅入口：可输入游标（上页返回的 `next_cursor`）、每页条数，
翻页并打开事件详情；批次事件详情页提供**原始 Parquet** 下载入口。
`/events/{id}` 是单条事件详情。

事件按提交顺序记录四类变更：

| 类型 | 内容 |
| --- | --- |
| `field_added` | 永久 `field_id`、字段名、结构版本 |
| `field_renamed` | 永久 `field_id`、`old_name`、`new_name`、结构版本 |
| `field_deleted` | 永久 `field_id`、删除前名称（`old_name`）、结构版本 |
| `batch_registered` | 批次 ID、行数、入湖时结构版本、入湖时字段的 ID 与名称、原始 Parquet 入口 |

语义保证：

- 游标 = 已读到的最后一条事件 ID；`GET /api/events?cursor=N` 返回 `id > N`
  的连续事件。重试同一游标是幂等的，不会跳过事件。
- 一页的事件与 `next_cursor` 来自同一个已提交读事务；翻页期间的新提交只会
  出现在后续页，已返回事件与游标不会改变。
- 事件与对应的字段变更 / 批次登记在同一 SQLite 事务提交：任一步失败则全部
  回滚，不会留下无事件的变更或事件孤儿；过期版本上传同样不产生事件。
- 事件行只追加：数据库触发器拒绝 UPDATE / DELETE，应用也从不改写历史行。
- 事件在进程重启后仍在原位置可续读（SQLite WAL 持久化，ID 由
  `AUTOINCREMENT` 永不回收）。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/state` | 结构快照 + 批次列表 |
| GET | `/healthz` | 实例名、pid、版本 |
| POST | `/api/fields` | `{"schema_version": v, "name": "..."}` |
| PATCH | `/api/fields/{field_id}` | 改名，body 同上 |
| DELETE | `/api/fields/{field_id}` | 软删除，body `{"schema_version": v}` |
| POST | `/api/batches` | multipart：`version` + `file` |
| GET | `/api/batches/{id}?format=json\|csv\|parquet` | 按当前结构读取/下载 |
| GET | `/api/batches/{id}/original` | 原样下载入湖时的 Parquet（不重写、不投影） |
| GET | `/api/events?cursor=N&limit=L` | 变更事件游标分页；返回 `events`、`next_cursor`、`has_more`、`latest_event_id`、`total_events`、`journal_start_version` |
| GET | `/api/events/{id}` | 单条事件 |
| GET | `/changes` | 变更订阅网页（输入游标、翻页、打开事件） |
| GET | `/events/{id}` | 事件详情网页（批次事件含原始下载入口） |
| GET | `/api/integrity` | 文件与登记一致性自检（含 `event_gaps`） |

过期版本返回 409 `{"error": "stale_schema", ...}`；参数错误 400；文件缺失 500 `integrity_error`。

## 测试

```bash
python -m pytest -q                 # 41 项：核心/网页/一致性/变更订阅；含两个真实 uvicorn 进程的并发测试
python scripts/selftest_dual.py     # 端到端演示脚本：起两个真实进程 + 共享目录，逐项打印 PASS/FAIL（含订阅续读/重试）
```

## 已知限制

- 单机 SQLite + 共享卷方案适用于演示与中小规模；跨主机需换成服务端数据库与对象存储。
- 网页为服务端渲染，无前端路由；并发冲突通过版本号拒绝，不做自动合并。
- 删除只做逻辑删除，Parquet 中历史字段的物理列会一直保留（这是“不重写历史”的代价）。
- 沙箱环境无 `docker`，Compose 配置与镜像构建未在本机实际运行；已用两个真实 uvicorn 进程共享目录完成同等并发验证。
