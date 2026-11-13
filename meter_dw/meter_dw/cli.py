"""命令行入口。

子命令：
  migrate   应用数据库迁移
  import    导入一批 JSONL 文件（登记检查点、分批事务处理）
  resume    续跑所有未完成文件（先核对 sha256，拒绝内容已变文件）
  status    查看任务运行、文件检查点、隔离与坏行统计、咨询锁状态
  report    查询北京时间自然日日报
  trace     追溯某天/某分钟对应的原始事件
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import BEIJING_TZ
from .db import LockBusy, connect, ensure_schema, lock_held_now
from .ingest import IngestError, run


def _cmd_migrate(_args: argparse.Namespace) -> int:
    ensure_schema()
    print("[migrate] 迁移检查完成")
    return 0


def _collect_files(paths: list[str]) -> list[Path]:
    files: list[Path] = []
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            files.extend(sorted(p.glob("*.jsonl")))
        else:
            files.append(p)
    if not files:
        raise SystemExit("未找到任何 .jsonl 输入文件")
    return files


def _cmd_import(args: argparse.Namespace) -> int:
    ensure_schema()
    files = _collect_files(args.paths)
    try:
        c = run("import", files)
    except LockBusy as exc:
        print(f"[拒绝] {exc}", file=sys.stderr)
        return 2
    except IngestError as exc:
        print(f"[错误] {exc}", file=sys.stderr)
        return 1
    print(
        f"[import] 文件完成 {c.files_done}/{len(files)}｜行 {c.lines_seen}｜"
        f"接受事件 {c.events_accepted}｜重复跳过 {c.duplicates_skipped}｜"
        f"隔离 {c.quarantined}｜坏行 {c.bad_lines}｜"
        f"增量更新日报格 {len(c.affected_dates)}"
    )
    return 0


def _cmd_resume(args: argparse.Namespace) -> int:
    ensure_schema()
    conn = connect(autocommit=True)
    try:
        rows = conn.execute(
            "SELECT file_name, source_path, status, last_line FROM meter.ingest_files "
            "WHERE status='processing' ORDER BY file_name"
        ).fetchall()
    finally:
        conn.close()

    if args.paths:
        # 显式指定文件：已登记的做摘要校验续跑，未登记的作为新文件纳入同一任务
        files = _collect_files(args.paths)
    else:
        if not rows:
            print("[resume] 没有未完成文件，无需续跑")
            return 0
        # 崩溃可能发生在首批之前：除 processing 文件外，已登记且未完成的文件
        # 同样自动纳入；调用方若需补齐从未登记的后续文件，可显式传路径。
        files = [Path(r["source_path"]) for r in rows]
        print(f"[resume] 发现 {len(rows)} 个未完成文件: "
              f"{[(r['file_name'], r['last_line']) for r in rows]}")
    try:
        c = run("resume", files)
    except LockBusy as exc:
        print(f"[拒绝] {exc}", file=sys.stderr)
        return 2
    except IngestError as exc:
        print(f"[错误] {exc}", file=sys.stderr)
        return 1
    print(
        f"[resume] 文件完成 {c.files_done}/{len(files)}｜续跑行 {c.lines_seen}｜"
        f"接受事件 {c.events_accepted}｜重复跳过 {c.duplicates_skipped}｜"
        f"隔离 {c.quarantined}｜坏行 {c.bad_lines}"
    )
    return 0


def _cmd_status(_args: argparse.Namespace) -> int:
    conn = connect(autocommit=True)
    try:
        print("== 咨询锁 ==")
        print("执行锁持有中:" , "是" if lock_held_now() else "否")

        print("\n== 最近任务运行 ==")
        for r in conn.execute(
            "SELECT run_id, task_type, status, files_requested, files_done, "
            "lines_seen, events_accepted, quarantined, bad_lines, error_message, "
            "started_at, finished_at FROM meter.task_runs ORDER BY run_id DESC LIMIT 10"
        ).fetchall():
            finished = (
                r["finished_at"].strftime("%Y-%m-%d %H:%M:%S")
                if r["finished_at"] else "-"
            )
            print(
                f"#{r['run_id']} {r['task_type']:<6} {r['status']:<9} "
                f"文件 {r['files_done']}/{r['files_requested']} 行 {r['lines_seen']} "
                f"接受 {r['events_accepted']} 隔离 {r['quarantined']} 坏行 {r['bad_lines']} "
                f"开始 {r['started_at']:%Y-%m-%d %H:%M:%S} 结束 {finished}"
            )
            if r["error_message"]:
                print(f"    └─ {r['error_message']}")

        print("\n== 文件检查点 ==")
        for r in conn.execute(
            "SELECT file_name, status, line_count, last_line, batches, sha256, "
            "imported_at, finished_at FROM meter.ingest_files ORDER BY file_name"
        ).fetchall():
            print(
                f"{r['file_name']:<28} {r['status']:<10} 进度 {r['last_line']}/{r['line_count']} "
                f"批次 {r['batches']} sha256={r['sha256'][:12]}…"
            )

        print("\n== 计数汇总 ==")
        for table in ("events", "meter_minute_state", "quarantine", "bad_lines",
                      "daily_report"):
            n = conn.execute(f"SELECT count(*) AS n FROM meter.{table}").fetchone()["n"]
            print(f"meter.{table:<20} {n}")

        print("\n== 隔离原因分布 ==")
        for r in conn.execute(
            "SELECT reason_code, count(*) AS n FROM meter.quarantine "
            "GROUP BY reason_code ORDER BY reason_code"
        ).fetchall():
            print(f"{r['reason_code']:<30} {r['n']}")
    finally:
        conn.close()
    return 0


def _cmd_report(args: argparse.Namespace) -> int:
    conn = connect(autocommit=True)
    try:
        sql = (
            "SELECT r.meter_id, r.report_date, r.valid_minutes, r.total_wh, "
            "       r.last_event_id, r.updated_at, "
            "       (SELECT count(*) FROM meter.events e "
            "         WHERE e.meter_id = r.meter_id "
            "           AND (e.minute_utc AT TIME ZONE 'Asia/Shanghai')::date "
            "               = r.report_date) AS source_events "
            "  FROM meter.daily_report r"
        )
        params: list = []
        where = []
        if args.meter:
            where.append("r.meter_id = %s")
            params.append(args.meter)
        if args.date:
            where.append("r.report_date = %s")
            params.append(args.date)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY r.report_date, r.meter_id"
        rows = conn.execute(sql, params).fetchall()
        if not rows:
            print("（无日报数据）")
            return 0
        print(f"{'电表':<12}{'北京日期':<12}{'有效分钟':>8}{'总用电Wh':>12}"
              f"{'原始事件数':>10}  最后事件")
        for r in rows:
            print(f"{r['meter_id']:<12}{str(r['report_date']):<12}"
                  f"{r['valid_minutes']:>8}{r['total_wh']:>12}"
                  f"{r['source_events']:>10}  {r['last_event_id']}")
    finally:
        conn.close()
    return 0


def _cmd_trace(args: argparse.Namespace) -> int:
    conn = connect(autocommit=True)
    try:
        print("== 日报格 -> 当前有效分钟 -> 原始事件 ==")
        state_sql = (
            "SELECT s.meter_id, "
            "       (s.minute_utc AT TIME ZONE 'Asia/Shanghai')::date AS bj_date, "
            "       to_char(s.minute_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS UTC') "
            "           AS minute_utc, s.revision, s.op, s.wh, s.event_id, "
            "       e.source_file, e.source_line, e.received_at "
            "  FROM meter.meter_minute_state s JOIN meter.events e "
            "    ON e.event_id = s.event_id"
        )
        params: list = []
        where = []
        if args.meter:
            where.append("s.meter_id = %s")
            params.append(args.meter)
        if args.date:
            where.append(
                "(s.minute_utc AT TIME ZONE 'Asia/Shanghai')::date = %s"
            )
            params.append(args.date)
        if where:
            state_sql += " WHERE " + " AND ".join(where)
        state_sql += " ORDER BY s.meter_id, bj_date, s.minute_utc"
        rows = conn.execute(state_sql, params).fetchall()
        for r in rows:
            wh = "-" if r["wh"] is None else r["wh"]
            print(
                f"{r['meter_id']} {r['bj_date']} {r['minute_utc']} "
                f"rev={r['revision']} {r['op']:<6} wh={wh:<6} "
                f"event={r['event_id']} <- {r['source_file']}:{r['source_line']}"
            )
        if not rows:
            print("（无匹配的有效状态行）")

        print("\n== 该范围内被隔离的冲突事件 ==")
        qsql = (
            "SELECT q.event_id, q.reason_code, q.reason_detail, q.source_file, "
            "       q.source_line, q.revision, q.op, q.wh "
            "  FROM meter.quarantine q"
        )
        qparams: list = []
        qwhere = []
        if args.meter:
            qwhere.append("q.meter_id = %s")
            qparams.append(args.meter)
        if args.date:
            qwhere.append("(q.minute_utc AT TIME ZONE 'Asia/Shanghai')::date = %s")
            qparams.append(args.date)
        if qwhere:
            qsql += " WHERE " + " AND ".join(qwhere)
        qsql += " ORDER BY q.source_file, q.source_line"
        qrows = conn.execute(qsql, qparams).fetchall()
        for r in qrows:
            print(f"[隔离] {r['event_id']} {r['reason_code']} "
                  f"{r['source_file']}:{r['source_line']} — {r['reason_detail']}")
        if not qrows:
            print("（无隔离记录）")
    finally:
        conn.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="meter-dw", description="电表分钟数据增量入仓")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("migrate", help="应用数据库迁移")
    sp.set_defaults(func=_cmd_migrate)

    sp = sub.add_parser("import", help="导入 JSONL 文件")
    sp.add_argument("paths", nargs="+", help="JSONL 文件或目录")
    sp.set_defaults(func=_cmd_import)

    sp = sub.add_parser("resume", help="续跑未完成文件（默认从检查点读取清单）")
    sp.add_argument("paths", nargs="*", help="可选：指定文件，否则续跑全部 processing 文件")
    sp.set_defaults(func=_cmd_resume)

    sp = sub.add_parser("status", help="任务与检查点状态")
    sp.set_defaults(func=_cmd_status)

    sp = sub.add_parser("report", help="查询北京时间日报")
    sp.add_argument("--meter", help="按电表过滤")
    sp.add_argument("--date", help="按北京日期过滤 YYYY-MM-DD")
    sp.set_defaults(func=_cmd_report)

    sp = sub.add_parser("trace", help="日报/分钟追溯到原始事件")
    sp.add_argument("--meter", help="按电表过滤")
    sp.add_argument("--date", help="按北京日期过滤 YYYY-MM-DD")
    sp.set_defaults(func=_cmd_trace)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
