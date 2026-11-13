"""全局配置：数据库连接、批大小、北京时区等。"""
from __future__ import annotations

import os
from zoneinfo import ZoneInfo

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# 会话级咨询锁键，保证同一时刻只有一个入仓执行者
ADVISORY_LOCK_KEY = 0x4D455445525F4457  # 'METER_DW' 的 64bit 释义空间

DEFAULT_BATCH_SIZE = 500

# 允许的操作集合与字段规则
VALID_OPS = ("upsert", "delete")


def dsn() -> str:
    """数据库连接串，优先使用环境变量 METER_DW_DSN。"""
    return os.environ.get(
        "METER_DW_DSN",
        "host=127.0.0.1 port=55432 dbname=meterdw user=node connect_timeout=10",
    )


def batch_size() -> int:
    try:
        value = int(os.environ.get("METER_DW_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))
        return max(1, value)
    except ValueError:
        return DEFAULT_BATCH_SIZE
