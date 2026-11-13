"""增量入仓核心：分批事务、版本决策、冲突隔离、检查点与可恢复处理。

事务边界：每个批次一个数据库事务，批次内的数据变更（events / 状态表 /
隔离表 / 坏行表 / 日报）与文件检查点更新在同一事务中提交。
进程被 SIGKILL 时：
- 已提交批次：检查点与数据同时生效，续跑跳过这些行；
- 未提交批次：整体回滚，检查点也不前进，续跑重新处理该批（幂等）。
"""
from __future__ import annotations

import hashlib
import os
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import psycopg

from .config import BEIJING_TZ, batch_size
from .db import connect, execution_lock
from .parsing import Event, LineError, parse_line

# 崩溃注入（仅用于验证强制终止恢复，正常运行不设置）
CRASH_IN_BATCH = "METER_DW_CRASH_IN_BATCH"      # 第 N 批提交前 kill
CRASH_AFTER_BATCH = "METER_DW_CRASH_AFTER_BATCH"  # 第 N 批提交后 kill
STARTUP_SLEEP = "METER_DW_STARTUP_SLEEP"        # 持锁后睡眠秒数（并发测试）


class IngestError(RuntimeError):
    """入仓过程中的不可恢复错误（如文件摘要不匹配）。"""


@dataclass
class RunCounters:
    lines_seen: int = 0
    events_accepted: int = 0
    duplicates_skipped: int = 0
    quarantined: int = 0
    bad_lines: int = 0
    files_done: int = 0
    affected_dates: set[tuple[str, date]] = field(default_factory=set)


def sha256_file(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def count_lines(path: Path) -> int:
    total = 0
    with path.open("rb") as fh:
        for _ in fh:
            total += 1
    return total


def _maybe_crash(env_var: str, batch_no: int, when: str) -> None:
    target = os.environ.get(env_var)
    if target and target.isdigit() and int(target) == batch_no:
        msg = f"[crash-inject] 第 {batch_no} 批{when}，模拟 SIGKILL（os._exit(137)）"
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()
        os._exit(137)


# --- 单事件版本决策 ---------------------------------------------------------

def _process_event(cur: psycopg.Cursor, ev: Event, counters: RunCounters) -> str:
    """在当前事务内处理一个事件，返回决策结果。

    决策优先级：
    1. event_id 已存在：同摘要=重复投递跳过；不同摘要=事件内容冲突隔离；
    2. 该 (电表, 分钟) 无状态：首条有效记录，直接落状态；
    3. 修订号更高：覆盖状态（含 upsert→delete、delete→upsert）；
    4. 修订号相同且业务内容一致：接受事件（留痕），状态不变；
    5. 修订号相同但内容冲突：保留首次有效记录，隔离 revision_conflict；
    6. 修订号更低：旧版重放，隔离 stale_revision_replay（删除墓碑同样拦截）。
    """
    # 1) 事件 ID 幂等 / 事件内容冲突
    row = cur.execute(
        "SELECT payload_sha256 FROM meter.events WHERE event_id = %s",
        [ev.event_id],
    ).fetchone()
    if row is not None:
        if row["payload_sha256"] == ev.payload_sha256:
            counters.duplicates_skipped += 1
            return "duplicate"
        cur.execute(
            "INSERT INTO meter.quarantine "
            "(reason_code, reason_detail, event_id, meter_id, minute_utc, revision, "
            " op, wh, payload_sha256, source_file, source_line) "
            "VALUES ('event_id_content_conflict', %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [
                f"同一 event_id={ev.event_id} 内容与首次有效记录不同 "
                f"(新摘要 {ev.payload_sha256[:12]} vs 已存 {row['payload_sha256'][:12]})",
                ev.event_id, ev.meter_id, ev.minute_utc, ev.revision,
                ev.op, ev.wh, ev.payload_sha256, ev.source_file, ev.source_line,
            ],
        )
        counters.quarantined += 1
        return "quarantined"

    # 2) 查当前有效状态
    state = cur.execute(
        "SELECT revision, op, wh FROM meter.meter_minute_state "
        "WHERE meter_id = %s AND minute_utc = %s",
        [ev.meter_id, ev.minute_utc],
    ).fetchone()

    if state is not None:
        if ev.revision < state["revision"]:
            cur.execute(
                "INSERT INTO meter.quarantine "
                "(reason_code, reason_detail, event_id, meter_id, minute_utc, revision, "
                " op, wh, payload_sha256, source_file, source_line) "
                "VALUES ('stale_revision_replay', %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [
                    f"修订号 {ev.revision} 低于当前有效版本 {state['revision']}，"
                    f"旧版重放被拒绝（防止删除后旧数据复活）",
                    ev.event_id, ev.meter_id, ev.minute_utc, ev.revision,
                    ev.op, ev.wh, ev.payload_sha256, ev.source_file, ev.source_line,
                ],
            )
            counters.quarantined += 1
            return "quarantined"

        if ev.revision == state["revision"]:
            same_content = (ev.op == state["op"]) and (
                ev.op == "delete" or ev.wh == state["wh"]
            )
            if not same_content:
                cur.execute(
                    "INSERT INTO meter.quarantine "
                    "(reason_code, reason_detail, event_id, meter_id, minute_utc, revision, "
                    " op, wh, payload_sha256, source_file, source_line) "
                    "VALUES ('revision_conflict', %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    [
                        f"同一修订号 {ev.revision} 业务内容冲突："
                        f"已存 op={state['op']},wh={state['wh']}；"
                        f"新到 op={ev.op},wh={ev.wh}，保留首次有效记录",
                        ev.event_id, ev.meter_id, ev.minute_utc, ev.revision,
                        ev.op, ev.wh, ev.payload_sha256, ev.source_file, ev.source_line,
                    ],
                )
                counters.quarantined += 1
                return "quarantined"
            # 同修订号同内容：接受事件留痕，状态与日报均不变
            cur.execute(
                "INSERT INTO meter.events "
                "(event_id, meter_id, minute_utc, revision, op, wh, payload_sha256, "
                " source_file, source_line) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [ev.event_id, ev.meter_id, ev.minute_utc, ev.revision, ev.op, ev.wh,
                 ev.payload_sha256, ev.source_file, ev.source_line],
            )
            counters.events_accepted += 1
            return "accepted"

        # 3) 更高修订号：事件落库 + 状态覆盖（delete 成为带版本的墓碑）
        cur.execute(
            "INSERT INTO meter.events "
            "(event_id, meter_id, minute_utc, revision, op, wh, payload_sha256, "
            " source_file, source_line) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [ev.event_id, ev.meter_id, ev.minute_utc, ev.revision, ev.op, ev.wh,
             ev.payload_sha256, ev.source_file, ev.source_line],
        )
        cur.execute(
            "UPDATE meter.meter_minute_state "
            "SET revision=%s, op=%s, wh=%s, event_id=%s, updated_at=now() "
            "WHERE meter_id=%s AND minute_utc=%s",
            [ev.revision, ev.op, ev.wh, ev.event_id, ev.meter_id, ev.minute_utc],
        )
    else:
        # 首条有效记录
        cur.execute(
            "INSERT INTO meter.events "
            "(event_id, meter_id, minute_utc, revision, op, wh, payload_sha256, "
            " source_file, source_line) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [ev.event_id, ev.meter_id, ev.minute_utc, ev.revision, ev.op, ev.wh,
             ev.payload_sha256, ev.source_file, ev.source_line],
        )
        cur.execute(
            "INSERT INTO meter.meter_minute_state "
            "(meter_id, minute_utc, revision, op, wh, event_id) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            [ev.meter_id, ev.minute_utc, ev.revision, ev.op, ev.wh, ev.event_id],
        )

    # 只增量重算受影响的（电表, 北京时间日期）这一格
    bj_date = ev.minute_utc.astimezone(BEIJING_TZ).date()
    cur.execute("SELECT meter.recompute_daily(%s, %s)", [ev.meter_id, bj_date])
    counters.affected_dates.add((ev.meter_id, bj_date))
    counters.events_accepted += 1
    return "accepted"


# --- 任务运行记录 -----------------------------------------------------------

def _start_run(conn: psycopg.Connection, task_type: str, n_files: int) -> int:
    # 锁已保证没有其他存活执行者；把历史残留的 running 标记为被中断
    with conn.transaction():
        conn.execute(
            "UPDATE meter.task_runs SET status='failed', finished_at=now(), "
            "error_message='进程中断，由后续运行接替（咨询锁保证无并发存活进程）' "
            "WHERE status='running'"
        )
        row = conn.execute(
            "INSERT INTO meter.task_runs (task_type, status, files_requested) "
            "VALUES (%s, 'running', %s) RETURNING run_id",
            [task_type, n_files],
        ).fetchone()
        return row["run_id"]


def _finish_run(conn: psycopg.Connection, run_id: int, status: str,
                counters: RunCounters, error: str | None = None) -> None:
    with conn.transaction():
        conn.execute(
            "UPDATE meter.task_runs SET status=%s, files_done=%s, lines_seen=%s, "
            "events_accepted=%s, quarantined=%s, bad_lines=%s, error_message=%s, "
            "finished_at=now() WHERE run_id=%s",
            [status, counters.files_done, counters.lines_seen,
             counters.events_accepted, counters.quarantined, counters.bad_lines,
             error, run_id],
        )


# --- 文件级处理 -------------------------------------------------------------

def _register_or_resume(cur: psycopg.Cursor, path: Path,
                        digest: str, size: int) -> tuple[str, int]:
    """返回 (处理方式, 已提交的最后行号)。摘要不匹配直接拒绝。"""
    row = cur.execute(
        "SELECT file_name, source_path, sha256, status, last_line "
        "FROM meter.ingest_files WHERE file_name = %s",
        [path.name],
    ).fetchone()
    if row is None:
        return ("new", 0)
    if row["sha256"] != digest:
        raise IngestError(
            f"文件 {path.name} 内容已变化：检查点摘要 {row['sha256'][:16]}… "
            f"!= 当前摘要 {digest[:16]}…，拒绝处理（不可变输入被破坏）"
        )
    if row["status"] == "done":
        return ("done", row["last_line"])
    return ("resume", row["last_line"])


def process_file(conn: psycopg.Connection, path: Path,
                 counters: RunCounters, global_batch_no: int) -> int:
    """处理单个文件，返回更新后的全局批序号。"""
    path = path.resolve()
    if not path.is_file():
        raise IngestError(f"输入文件不存在: {path}")

    digest, size = sha256_file(path)
    total_lines = count_lines(path)

    with conn.transaction():
        cur = conn.cursor()
        mode, resume_from = _register_or_resume(cur, path, digest, size)
        if mode == "done":
            print(f"[skip] {path.name} 已完成（检查点 last_line={resume_from}）")
            counters.files_done += 1
            return global_batch_no
        if mode == "new":
            cur.execute(
                "INSERT INTO meter.ingest_files "
                "(file_name, source_path, sha256, byte_size, line_count, status, last_line) "
                "VALUES (%s,%s,%s,%s,%s,'processing',0)",
                [path.name, str(path), digest, size, total_lines],
            )
            print(f"[new ] {path.name} {total_lines} 行 sha256={digest[:16]}…")
        else:
            print(f"[res] {path.name} 从检查点 last_line={resume_from} 续跑 "
                  f"sha256 校验通过")
    # 登记/校验随 with 块独立提交，避免与首批数据互相拖累

    batch: list[Event] = []
    bad: list[tuple[int, str, str]] = []
    last_line_in_batch = 0

    def flush() -> int:
        nonlocal global_batch_no, batch, bad, last_line_in_batch
        if not batch and not bad:
            return global_batch_no
        global_batch_no += 1
        batch_no = global_batch_no
        with conn.transaction():
            cur = conn.cursor()
            for ev in batch:
                _process_event(cur, ev, counters)
            for line_no, raw_text, detail in bad:
                cur.execute(
                    "INSERT INTO meter.bad_lines (source_file, source_line, raw_text, reason_detail) "
                    "VALUES (%s,%s,%s,%s)",
                    [path.name, line_no, raw_text[:4000], detail],
                )
            # 检查点与本批数据同一事务推进
            cur.execute(
                "UPDATE meter.ingest_files SET last_line=%s, batches=batches+1 "
                "WHERE file_name=%s",
                [last_line_in_batch, path.name],
            )
            _maybe_crash(CRASH_IN_BATCH, batch_no, "提交前")
        _maybe_crash(CRASH_AFTER_BATCH, batch_no, "提交后")
        batch = []
        bad = []
        return global_batch_no

    with path.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            if line_no <= resume_from:
                continue
            counters.lines_seen += 1
            last_line_in_batch = line_no
            stripped = raw.strip()
            if not stripped:
                bad.append((line_no, raw.rstrip("\n"), "空行"))
                counters.bad_lines += 1
            else:
                try:
                    ev = parse_line(stripped, path.name, line_no)
                except LineError as exc:
                    bad.append((line_no, stripped, str(exc)))
                    counters.bad_lines += 1
                else:
                    batch.append(ev)
            if len(batch) + len(bad) >= batch_size():
                global_batch_no = flush()

    global_batch_no = flush()

    with conn.transaction():
        conn.execute(
            "UPDATE meter.ingest_files SET status='done', finished_at=now(), "
            "line_count=%s WHERE file_name=%s AND status='processing'",
            [total_lines, path.name],
        )
    counters.files_done += 1
    print(f"[done] {path.name} 处理完毕")
    return global_batch_no


def run(task_type: str, files: list[Path]) -> RunCounters:
    """加全局咨询锁后依次处理文件，返回计数器。"""
    counters = RunCounters()
    # autocommit 连接持有会话咨询锁；with conn.transaction() 在内层开启
    # 真正的事务，保证批次原子提交。
    conn = connect(autocommit=True)
    try:
        with execution_lock(conn):
            sleep_s = os.environ.get(STARTUP_SLEEP)
            if sleep_s:
                import time

                print(f"[lock] 已持有执行锁，按 {STARTUP_SLEEP} 睡眠 {sleep_s}s …")
                time.sleep(float(sleep_s))

            run_id = _start_run(conn, task_type, len(files))
            try:
                global_batch_no = 0
                for path in files:
                    global_batch_no = process_file(
                        conn, Path(path), counters, global_batch_no
                    )
            except Exception as exc:
                _finish_run(conn, run_id, "failed", counters, str(exc))
                raise
            _finish_run(conn, run_id, "succeeded", counters)
    finally:
        conn.close()
    return counters
