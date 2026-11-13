#!/usr/bin/env python3
"""端到端验证脚本。

覆盖题面要求的全部场景：
1. 跨北京时间零点的数据归属；
2. 次日修订前日用电量；
3. 删除后重放旧版不能复活；
4. 重复与冲突记录混入坏行；
5. 批次提交前/后强制终止（SIGKILL）再恢复，恢复结果与不中断处理一致；
6. 续跑前文件摘要核对，拒绝内容已变的文件；
7. 同一任务并发启动只允许一个执行者。

方法：
- GOLDEN 库：不中断地导入全部文件，导出全部业务表快照作为基准；
- CRASH 库：分别在“提交前”“提交后”注入 SIGKILL，再 resume 到完成，
  逐表与 GOLDEN 比对（时间戳类列归一化后比对）；
- 全程不使用清空数据库或全量重算。
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent
VENV_PY = ROOT.parent / ".venv" / "bin" / "python"
DATA = ROOT.parent / "data"
INCOMING = DATA / "incoming"
WORK = DATA / "verify_work"

BASE_DSN = "host=127.0.0.1 port=55432 user=node connect_timeout=10"

TABLES = [
    "events",
    "meter_minute_state",
    "quarantine",
    "bad_lines",
    "daily_report",
    "ingest_files",
]
# 比对时忽略/归一化的易变列
NORMALIZE = {
    "ingest_files": ["imported_at", "finished_at", "batches"],
    "task_runs": ["started_at", "finished_at", "run_id", "error_message"],
    "daily_report": ["updated_at"],
    "events": ["received_at"],
    "meter_minute_state": ["updated_at"],
    "quarantine": ["created_at", "id"],
    "bad_lines": ["created_at", "id"],
}

failures: list[str] = []
passes: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        passes.append(name)
        print(f"  ✅ {name} {detail}")
    else:
        failures.append(f"{name} {detail}")
        print(f"  ❌ {name} {detail}")


def psql_admin(sql: str, db: str = "postgres") -> None:
    subprocess.run(
        ["/usr/lib/postgresql/16/bin/psql", "-h", "127.0.0.1", "-p", "55432",
         "-U", "node", "-d", db, "-q", "-c", sql],
        check=True, capture_output=True, text=True,
    )


def reset_db(name: str) -> None:
    psql_admin(f'DROP DATABASE IF EXISTS {name}')
    psql_admin(f'CREATE DATABASE {name}')


def cli(db: str, *args: str, env_extra: dict | None = None,
        expect_rc: int | None = None) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["METER_DW_DSN"] = f"{BASE_DSN} dbname={db}"
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(
        [str(VENV_PY), "-m", "meter_dw", *args],
        cwd=ROOT, env=env, capture_output=True, text=True,
    )
    if expect_rc is not None:
        check(f"[rc={expect_rc}] {' '.join(args)}", proc.returncode == expect_rc,
              f"实际 rc={proc.returncode} stderr={proc.stderr.strip()[:200]}")
    elif proc.returncode != 0:
        failures.append(f"命令失败: {' '.join(args)}\n{proc.stderr}")
        print(proc.stdout)
        print(proc.stderr)
        raise SystemExit(1)
    return proc


def snapshot(db: str) -> dict[str, list[dict]]:
    conn = psycopg.connect(f"{BASE_DSN} dbname={db}", autocommit=True)
    out: dict[str, list[dict]] = {}
    try:
        for table in TABLES:
            cols = [
                r[0] for r in conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='meter' AND table_name=%s ORDER BY ordinal_position",
                    [table],
                ).fetchall()
            ]
            keep = [c for c in cols if c not in NORMALIZE.get(table, [])]
            rows = conn.execute(
                f"SELECT {', '.join(keep)} FROM meter.{table} "
                f"ORDER BY {', '.join(keep)}"
            ).fetchall()
            out[table] = [dict(zip(keep, row)) for row in rows]
    finally:
        conn.close()
    return out


def diff_snapshots(a: dict, b: dict, label: str) -> bool:
    ok = True
    for table in TABLES:
        if a[table] != b[table]:
            ok = False
            print(f"  ❌ [{label}] 表 meter.{table} 不一致")
            sa, sb = a[table], b[table]
            print(f"     行数 golden={len(sa)} crash={len(sb)}")
            for ra, rb in zip(sa, sb):
                if ra != rb:
                    print(f"     golden: {ra}")
                    print(f"     crash : {rb}")
                    break
        else:
            print(f"  ✅ [{label}] 表 meter.{table} 一致（{len(a[table])} 行）")
    if ok:
        passes.append(label)
    else:
        failures.append(label)
    return ok


def expected_golden_assertions(db: str) -> None:
    """对金库里的业务预期做显式断言。"""
    conn = psycopg.connect(f"{BASE_DSN} dbname={db}", autocommit=True)
    try:
        rep = {
            (r[0], str(r[1])): (r[2], r[3])
            for r in conn.execute(
                "SELECT meter_id, report_date, valid_minutes, total_wh "
                "FROM meter.daily_report ORDER BY meter_id, report_date"
            ).fetchall()
        }
        # 场景1：跨零点归属（UTC 15:59 -> 北京 11-12；16:00/16:01 -> 11-13）
        check("跨零点 M1/2026-11-12 = (1 分钟, 105Wh)",
              rep.get(("M1", "2026-11-12")) == (1, 105), str(rep.get(("M1", "2026-11-12"))))
        # 场景2：次日修订（115+120+300=535）
        check("次日修订后 M1/2026-11-13 = (3 分钟, 535Wh)",
              rep.get(("M1", "2026-11-13")) == (3, 535), str(rep.get(("M1", "2026-11-13"))))
        # 场景3：删除生效且不复活，M2 只剩 16:02=220
        check("删除后 M2/2026-11-13 = (1 分钟, 220Wh)",
              rep.get(("M2", "2026-11-13")) == (1, 220), str(rep.get(("M2", "2026-11-13"))))
        check("M3/2026-11-13 = (2 分钟, 810Wh)",
              rep.get(("M3", "2026-11-13")) == (2, 810), str(rep.get(("M3", "2026-11-13"))))

        # 场景4：坏行 7、隔离 6、事件 14、状态 9（M1x4 + M2x3 + M3x2）
        for table, expected in [("bad_lines", 7), ("quarantine", 6),
                                ("events", 14), ("meter_minute_state", 9)]:
            n = conn.execute(f"SELECT count(*) FROM meter.{table}").fetchone()[0]
            check(f"meter.{table} 行数 = {expected}", n == expected, f"实际 {n}")

        # 删除墓碑确实保留了版本（rev=2, op=delete）
        tomb = conn.execute(
            "SELECT revision, op FROM meter.meter_minute_state "
            "WHERE meter_id='M2' AND minute_utc='2026-11-12T16:01:00Z'"
        ).fetchone()
        check("M2 16:01 删除墓碑保留版本 (rev=2, delete)",
              tomb == (2, "delete"), str(tomb))

        # 隔离原因分布
        reasons = dict(conn.execute(
            "SELECT reason_code, count(*) FROM meter.quarantine GROUP BY reason_code"
        ).fetchall())
        check("隔离原因分布正确",
              reasons == {"event_id_content_conflict": 1,
                          "revision_conflict": 1,
                          "stale_revision_replay": 4},
              str(reasons))
    finally:
        conn.close()


def build_golden() -> dict:
    print("\n========== 1. 构建 GOLDEN（不中断基线） ==========")
    reset_db("meterdw_golden")
    cli("meterdw_golden", "migrate")
    files = sorted(str(p) for p in INCOMING.glob("batch*.jsonl"))
    cli("meterdw_golden", "import", *files)
    expected_golden_assertions("meterdw_golden")
    return snapshot("meterdw_golden")


def crash_scenario(crash_env: str, batch_target: str, label: str,
                   golden: dict) -> None:
    print(f"\n========== 崩溃恢复：{label} ==========")
    db = "meterdw_crash"
    reset_db(db)
    cli(db, "migrate")
    # 小批大小制造更多批次；用三个文件依次导入，崩溃注入按全局批次号
    env = {"METER_DW_BATCH_SIZE": "3", crash_env: batch_target}
    proc = cli(db, "import",
               *sorted(str(p) for p in INCOMING.glob("batch*.jsonl")),
               env_extra=env, expect_rc=137)
    check("进程以 137 退出（SIGKILL 注入）",
          "crash-inject" in proc.stderr or proc.returncode == 137,
          proc.stderr.strip()[:120])

    # 中断态检查点
    conn = psycopg.connect(f"{BASE_DSN} dbname={db}", autocommit=True)
    try:
        proc_files = conn.execute(
            "SELECT file_name, status, last_line, batches FROM meter.ingest_files "
            "ORDER BY file_name"
        ).fetchall()
        print("  中断时检查点:", proc_files)
        running = conn.execute(
            "SELECT count(*) FROM meter.task_runs WHERE status='running'"
        ).fetchone()[0]
        check("中断后存在 running 任务记录", running >= 1, f"running={running}")
    finally:
        conn.close()

    # 恢复：先验证“未篡改可正常续跑”。显式传入全部文件——processing 的按
    # 检查点续跑，崩溃前未登记的文件在同一任务中补登并处理。
    all_files = sorted(str(p) for p in INCOMING.glob("batch*.jsonl"))
    proc2 = cli(db, "resume", *all_files)
    print("  ", proc2.stdout.strip().replace("\n", "\n   "))

    conn = psycopg.connect(f"{BASE_DSN} dbname={db}", autocommit=True)
    try:
        left = conn.execute(
            "SELECT count(*) FROM meter.ingest_files WHERE status='processing'"
        ).fetchone()[0]
        check("续跑后无 processing 文件", left == 0, f"剩余 {left}")
        interrupted = conn.execute(
            "SELECT count(*) FROM meter.task_runs WHERE status='failed' "
            "AND error_message LIKE '%进程中断%'"
        ).fetchone()[0]
        check("上次 running 记录被标记为中断接替", interrupted >= 1, f"计数 {interrupted}")
    finally:
        conn.close()

    diff_snapshots(snapshot(db), golden, label)


def tamper_scenario() -> None:
    print("\n========== 篡改文件摘要拒绝 ==========")
    db = "meterdw_tamper"
    reset_db(db)
    cli(db, "migrate")
    work = WORK / "tamper"
    work.mkdir(parents=True, exist_ok=True)
    f = work / "tampered.jsonl"
    shutil.copy(INCOMING / "tampered.jsonl", f)
    before = f.read_bytes()

    # 只导入第一行所在的首块：通过先复制一个“前置文件”，让 tampered 处于 processing
    # 更直接的做法：正常导入后，将状态改回 processing 再篡改，模拟中断后文件被改
    cli(db, "import", str(f))
    conn = psycopg.connect(f"{BASE_DSN} dbname={db}", autocommit=True)
    try:
        conn.execute(
            "UPDATE meter.ingest_files SET status='processing', last_line=1 "
            "WHERE file_name='tampered.jsonl'"
        )
    finally:
        conn.close()

    # 篡改一个字节（保持 JSON 行数不变，证明是摘要而不是行数在把关）
    tampered = before.replace(b'"wh": 20', b'"wh": 21', 1)
    check("篡改确实改变了字节", tampered != before)
    f.write_bytes(tampered)

    proc = cli(db, "resume", str(f), expect_rc=1)
    check("摘要不匹配被拒绝", "内容已变化" in proc.stderr or "内容已变化" in proc.stdout,
          proc.stderr.strip()[:160])

    # 恢复原字节后应可正常续跑
    f.write_bytes(before)
    cli(db, "resume", str(f))
    conn = psycopg.connect(f"{BASE_DSN} dbname={db}", autocommit=True)
    try:
        st = conn.execute(
            "SELECT status, last_line FROM meter.ingest_files WHERE file_name='tampered.jsonl'"
        ).fetchone()
        check("还原字节后续跑完成", st == ("done", 2), str(st))
        # 篡改期间的数据没有混入
        wh = conn.execute(
            "SELECT wh FROM meter.meter_minute_state "
            "WHERE meter_id='M9' AND minute_utc='2026-11-12T16:21:00Z'"
        ).fetchone()[0]
        check("被篡改的 wh=21 从未入库（仍为原值 20）", wh == 20, f"wh={wh}")
    finally:
        conn.close()


def concurrency_scenario() -> None:
    print("\n========== 并发启动互斥 ==========")
    db = "meterdw_conc"
    reset_db(db)
    cli(db, "migrate")
    env = os.environ.copy()
    env["METER_DW_DSN"] = f"{BASE_DSN} dbname={db}"
    env["METER_DW_STARTUP_SLEEP"] = "5"

    files = sorted(str(p) for p in INCOMING.glob("batch*.jsonl"))
    p1 = subprocess.Popen(
        [str(VENV_PY), "-m", "meter_dw", "import", *files],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(1.5)  # 确保 p1 已持锁
    p2 = subprocess.Popen(
        [str(VENV_PY), "-m", "meter_dw", "import", *files],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    out2, err2 = p2.communicate(timeout=30)
    out1, err1 = p1.communicate(timeout=30)
    check("第二个执行者被拒绝（rc=2）", p2.returncode == 2, f"rc={p2.returncode}")
    check("拒绝原因是锁忙", "另一个入仓进程" in err2, err2.strip()[:160])
    check("持锁进程正常成功（rc=0）", p1.returncode == 0, f"rc={p1.returncode}")
    check("数据只被处理一次（events=14）",
          "接受事件 14" in out1, out1.strip().splitlines()[-1])


def idempotent_reimport() -> None:
    print("\n========== 重复导入幂等 ==========")
    db = "meterdw_idem"
    reset_db(db)
    cli(db, "migrate")
    files = sorted(str(p) for p in INCOMING.glob("batch*.jsonl"))
    cli(db, "import", *files)
    proc = cli(db, "import", *files)
    check("再次导入全部文件被跳过", proc.stdout.count("[skip]") == 3,
          f"skip 次数={proc.stdout.count('[skip]')}")
    conn = psycopg.connect(f"{BASE_DSN} dbname={db}", autocommit=True)
    try:
        n = conn.execute("SELECT count(*) FROM meter.events").fetchone()[0]
        check("重复导入不产生额外事件", n == 14, f"events={n}")
    finally:
        conn.close()


def full_day_deletion() -> None:
    print("\n========== 整天有效分钟全部删除：日报格移除 ==========")
    db = "meterdw_delday"
    reset_db(db)
    cli(db, "migrate")
    work = WORK / "delday"
    work.mkdir(parents=True, exist_ok=True)
    f = work / "delday.jsonl"
    rows = [
        '{"event_id":"d1","meter_id":"MD","minute_utc":"2026-11-12T16:30:00Z","revision":1,"op":"upsert","wh":10}',
        '{"event_id":"d2","meter_id":"MD","minute_utc":"2026-11-12T16:31:00Z","revision":1,"op":"upsert","wh":20}',
        '{"event_id":"d3","meter_id":"MD","minute_utc":"2026-11-12T16:30:00Z","revision":2,"op":"delete"}',
        '{"event_id":"d4","meter_id":"MD","minute_utc":"2026-11-12T16:31:00Z","revision":2,"op":"delete"}',
    ]
    f.write_text("\n".join(rows) + "\n", encoding="utf-8")
    cli(db, "import", str(f))
    conn = psycopg.connect(f"{BASE_DSN} dbname={db}", autocommit=True)
    try:
        n = conn.execute(
            "SELECT count(*) FROM meter.daily_report WHERE meter_id='MD'"
        ).fetchone()[0]
        check("全部分钟删除后不存在 0 值幽灵日报格", n == 0, f"报告行数={n}")
        states = conn.execute(
            "SELECT revision, op FROM meter.meter_minute_state "
            "WHERE meter_id='MD' ORDER BY minute_utc"
        ).fetchall()
        check("两条删除墓碑仍保留版本", states == [(2, "delete"), (2, "delete")],
              str(states))
        # 旧版重放不能让该日复活
        f2 = work / "replay.jsonl"
        f2.write_text(
            '{"event_id":"d5","meter_id":"MD","minute_utc":"2026-11-12T16:30:00Z",'
            '"revision":1,"op":"upsert","wh":10}\n',
            encoding="utf-8",
        )
        cli(db, "import", str(f2))
        n2 = conn.execute(
            "SELECT count(*) FROM meter.daily_report WHERE meter_id='MD'"
        ).fetchone()[0]
        check("旧版重放后日报仍不复活", n2 == 0, f"报告行数={n2}")
        q = conn.execute(
            "SELECT reason_code FROM meter.quarantine WHERE event_id='d5'"
        ).fetchone()
        check("重放行被隔离为 stale_revision_replay",
              q == ("stale_revision_replay",), str(q))
    finally:
        conn.close()


def main() -> int:
    if WORK.exists():
        shutil.rmtree(WORK)
    golden = build_golden()
    # 场景5a：提交前崩溃 -> 整个批次回滚，检查点不前进
    crash_scenario("METER_DW_CRASH_IN_BATCH", "2", "提交前 SIGKILL（批次回滚）", golden)
    # 场景5b：提交后崩溃 -> 已提交批次保留，从未提交边界续跑
    crash_scenario("METER_DW_CRASH_AFTER_BATCH", "2", "提交后 SIGKILL（已提交保留）", golden)
    tamper_scenario()
    concurrency_scenario()
    idempotent_reimport()
    full_day_deletion()

    print("\n================ 汇总 ================")
    print(f"通过 {len(passes)} 项，失败 {len(failures)} 项")
    if failures:
        print("失败项:")
        for f in failures:
            print(" -", f)
        return 1
    print("🎉 全部验证通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
