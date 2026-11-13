# 园区电表分钟数据 · 可恢复增量入仓

Python 3.11 + PostgreSQL 16 实现。输入为不可变 JSONL 文件，支持设备断网补传、
次日计量修正、删除重放防护、崩溃后从检查点恢复、并发单执行者、北京时间日报。

## 1. 目录结构

```
meter_dw/
├── migrations/0001_init.sql      # 数据库迁移（事件/状态/隔离/坏行/检查点/日报）
├── meter_dw/
│   ├── config.py                 # DSN、批大小、北京时区、咨询锁键
│   ├── db.py                     # 连接、幂等迁移、会话咨询锁
│   ├── parsing.py                # JSONL 严格解析、规范化 sha256 摘要
│   ├── ingest.py                 # 分批事务、版本决策、检查点、崩溃注入点
│   └── cli.py                    # migrate/import/resume/status/report/trace
├── scripts/
│   ├── generate_data.py          # 合成三批输入 + 篡改验证文件
│   └── verify.py                 # 端到端验证（37 项断言，含 SIGKILL 恢复）
├── requirements.txt
└── pyproject.toml
```

## 2. 本地独立数据库（一次性）

普通用户、独立数据目录、独立端口、仅监听 loopback：

```sh
PG_BIN=/usr/lib/postgresql/16/bin
$PG_BIN/initdb -D ../pgdata -E UTF8 --locale=C -U node --auth=trust
printf "port = 55432\nlisten_addresses = '127.0.0.1'\nunix_socket_directories = '/tmp'\n" \
  >> ../pgdata/postgresql.conf
$PG_BIN/pg_ctl -D ../pgdata -l ../pgrun/pg.log -w start
$PG_BIN/createdb -h 127.0.0.1 -p 55432 -U node meterdw
# 停止：$PG_BIN/pg_ctl -D ../pgdata stop
```

应用依赖与连接串：

```sh
python3.11 -m venv ../.venv
../.venv/bin/pip install -r requirements.txt
export METER_DW_DSN="host=127.0.0.1 port=55432 dbname=meterdw user=node connect_timeout=10"
# 不设置时使用上述默认值
```

## 3. 命令

```sh
PY=../.venv/bin/python

$PY -m meter_dw migrate                       # 应用迁移（幂等）
$PY -m meter_dw import <文件或目录>...         # 导入，支持一次多文件
$PY -m meter_dw resume [文件...]              # 续跑；不带参数续跑全部 processing 文件
$PY -m meter_dw status                        # 锁/任务/检查点/计数/隔离原因
$PY -m meter_dw report [--meter M1] [--date 2026-11-13]
$PY -m meter_dw trace  [--meter M1] [--date 2026-11-13]   # 日报回溯原始事件
```

环境变量：

| 变量 | 作用 |
| --- | --- |
| `METER_DW_DSN` | 数据库连接串 |
| `METER_DW_BATCH_SIZE` | 每批行数（默认 500，验证时调小制造多批次） |
| `METER_DW_CRASH_IN_BATCH=N` | 第 N 批**提交前** `os._exit(137)`（仅测试） |
| `METER_DW_CRASH_AFTER_BATCH=N` | 第 N 批**提交后** `os._exit(137)`（仅测试） |
| `METER_DW_STARTUP_SLEEP=S` | 持锁后睡眠 S 秒（并发互斥测试） |

## 4. 数据模型与核心规则

- `meter.events`：首次有效处理的事件（事件溯源，含源文件/行号/规范化摘要）。
- `meter.meter_minute_state`：每表每分钟**只保留最高修订号**有效记录；
  `delete` 以带修订号的墓碑行存在，旧版重放无法复活数据。
- `meter.quarantine`：冲突隔离，原因码
  `event_id_content_conflict` / `revision_conflict` / `stale_revision_replay`。
- `meter.bad_lines`：格式错误行，记录文件名、行号、原文、原因，不阻断后续行。
- `meter.ingest_files`：文件级检查点（sha256、字节数、行数、last_line、批次号）。
- `meter.daily_report`：北京时间（`Asia/Shanghai`）自然日日报，
  由 `meter.recompute_daily(电表, 日期)` 在每次状态变更后**只重算受影响的一格**；
  某日有效 upsert 全部删除后该报告格移除，不留 0 值幽灵行。

决策优先级（同一连接的单事务内完成）：

1. `event_id` 已存在：摘要相同 → 重复跳过；摘要不同 → 隔离；
2. 同表同分钟无状态：首条有效记录落库；
3. 修订号更高：覆盖状态（upsert→delete、delete→upsert 均可）；
4. 修订号相同且业务内容一致：事件留痕、状态不变；
5. 修订号相同但内容冲突：保留首次有效记录，新行隔离；
6. 修订号更低：隔离 `stale_revision_replay`（删除墓碑同样拦截）。

## 5. 事务、检查点与恢复语义

- 每个批次一个事务：事件、状态、隔离、坏行、日报与 `last_line` 检查点
  **同事务原子提交**。
- SIGKILL 后：已提交批次（含检查点）生效；未提交批次整体回滚、检查点不前进。
- `resume` 先重新计算文件 sha256 与检查点比对，**内容已变直接拒绝**；
  校验通过才从 `last_line+1` 重新处理，重复事件由 event_id 幂等兜底。
- 全局会话级咨询锁 `pg_try_advisory_lock` 保证同一时刻只有一个执行者；
  进程死亡后 PostgreSQL 自动回收会话锁，无僵死锁。
- 恢复路径**不清库、不全量重算**：日报仅对状态变更涉及的（电表, 北京日期）格重算。

## 6. 合成数据与一键验证

```sh
$PY scripts/generate_data.py     # 生成 ../data/incoming/batch{1,2,3}*.jsonl
$PY scripts/verify.py            # 37 项断言，独立测试库，不影响 meterdw
```

验证覆盖：

1. 跨北京零点归属（UTC 15:59→前一日，16:00→次日）；
2. 次日高修订号修正前日用电量；
3. 删除后重放旧版被隔离、数据不复活；
4. 重复投递、同事件内容冲突、同修订号业务冲突与 7 类坏行混入；
5. 提交前/提交后 SIGKILL 再 resume，六张业务表与不中断 GOLDEN 逐行比对；
6. 续跑前 sha256 不匹配拒绝、还原后可续跑且篡改值从未入库；
7. 并发启动第二个执行者被锁拒绝（rc=2）；
8. 重复导入幂等、整天删除后报告格移除且旧版重放不复活。

## 7. 输入行格式

```json
{"event_id":"e-1","meter_id":"M1","minute_utc":"2026-11-12T16:00:00Z",
 "revision":1,"op":"upsert","wh":110}
{"event_id":"e-2","meter_id":"M1","minute_utc":"2026-11-12T16:01:00Z",
 "revision":2,"op":"delete"}
```

约束：整分钟 UTC 时间（秒=0）；revision 正整数；upsert 的 wh 为非负整数且必填；
delete 不得携带 wh；不允许未知字段；同 event_id 重复投递按规范化 sha256 判定。
