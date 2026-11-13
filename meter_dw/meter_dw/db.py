"""PostgreSQL 连接、迁移执行与全局咨询锁。"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from .config import ADVISORY_LOCK_KEY, dsn

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


def connect(autocommit: bool = False) -> psycopg.Connection:
    # 注意：不能在连接后立刻执行 SET，否则会先开启一个外层事务，
    # 使后续 with conn.transaction() 退化为保存点、退出时不提交。
    # 通过 options 在连接建立时设置会话参数。
    conn = psycopg.connect(
        dsn() + " options='-c TimeZone=UTC'",
        row_factory=dict_row,
        autocommit=autocommit,
    )
    return conn


def ensure_schema() -> None:
    """幂等执行 migrations/ 下按文件名排序的 SQL 迁移。"""
    conn = connect(autocommit=True)
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS meter")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS meter.schema_migrations ("
            "version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        )
        applied = {
            r["version"]
            for r in conn.execute("SELECT version FROM meter.schema_migrations").fetchall()
        }
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version = path.stem
            if version in applied:
                continue
            sql = path.read_text(encoding="utf-8")
            # 迁移文件自带 BEGIN/COMMIT，使用简单查询协议整体提交
            conn.execute(sql)
            print(f"[migrate] 已应用 {version}")
    finally:
        conn.close()


class LockBusy(RuntimeError):
    """已有另一个入仓进程持有执行锁。"""


@contextmanager
def execution_lock(conn: psycopg.Connection):
    """会话级排他咨询锁：同一任务并发启动时只允许一个执行者。

    连接必须工作在 autocommit 模式：pg_try_advisory_lock 本身会开启事务，
    若连接处于普通事务模式，后续 with conn.transaction() 会退化为保存点、
    退出时不提交。autocommit 下每条语句独立事务，应用层的事务块可正常提交。
    锁在会话结束（含进程被 kill 后由 PostgreSQL 自动回收会话）时释放，
    不会产生僵死锁。
    """
    if not conn.autocommit:
        raise RuntimeError("execution_lock 要求 autocommit 连接")
    got = conn.execute("SELECT pg_try_advisory_lock(%s)", [ADVISORY_LOCK_KEY]).fetchone()[
        "pg_try_advisory_lock"
    ]
    if not got:
        raise LockBusy("另一个入仓进程正在运行，本次启动被拒绝")
    try:
        yield
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", [ADVISORY_LOCK_KEY])


def lock_held_now() -> bool:
    """供 status 命令观察锁状态（独立连接尝试加锁后立即释放）。"""
    conn = connect(autocommit=True)
    try:
        got = conn.execute(
            "SELECT pg_try_advisory_lock(%s) AS got", [ADVISORY_LOCK_KEY]
        ).fetchone()["got"]
        if got:
            conn.execute("SELECT pg_advisory_unlock(%s)", [ADVISORY_LOCK_KEY])
        return not got
    finally:
        conn.close()
