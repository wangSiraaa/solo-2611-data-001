"""JSONL 行解析与严格校验。

校验规则（任何不满足都记为坏行，记录文件名与行号，不阻断后续行）：
- 必须是 JSON 对象；
- 必含字段 event_id / meter_id / minute_utc / revision / op；
- event_id、meter_id 为非空字符串；
- minute_utc 为 UTC 分钟时间：接受 'YYYY-MM-DDTHH:MM:SSZ' / '...+00:00'，
  秒与微秒必须为 0（分钟级数据）；
- revision 为正整数；
- op 仅允许 upsert / delete；
- upsert 必含非负整数 wh；delete 不允许携带 wh；
- 不允许未知字段；不允许布尔/浮点等非整数类型冒充整数。
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import BEIJING_TZ, VALID_OPS

_REQUIRED = ("event_id", "meter_id", "minute_utc", "revision", "op")
_KNOWN_FIELDS = frozenset(_REQUIRED + ("wh",))

# 严格的 UTC 分钟时间格式
_UTC_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-]00:?00)$"
)


class LineError(ValueError):
    """单行格式/语义校验失败。"""


@dataclass(frozen=True)
class Event:
    event_id: str
    meter_id: str
    minute_utc: datetime          # 带 tzinfo=UTC
    revision: int
    op: str
    wh: int | None
    payload_sha256: str
    source_file: str
    source_line: int

    def beijing_date(self) -> "datetime.date":  # noqa: F821 (typing only)
        from datetime import date

        d = self.minute_utc.astimezone(BEIJING_TZ)
        return date(d.year, d.month, d.day)


def canonical_payload(raw: dict[str, Any]) -> bytes:
    """对业务字段做规范化序列化，保证同内容事件摘要稳定。

    只纳入业务字段（不含文件、行号等投递信息），按 key 排序、
    不转义非 ASCII、统一分隔符。
    """
    business = {k: raw[k] for k in sorted(raw) if k in _KNOWN_FIELDS}
    return json.dumps(
        business, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def payload_hash(raw: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_payload(raw)).hexdigest()


def _parse_minute(value: Any) -> datetime:
    if not isinstance(value, str):
        raise LineError("minute_utc 必须是字符串")
    m = _UTC_RE.match(value)
    if not m:
        raise LineError(f"minute_utc 格式非法: {value!r}")
    year, month, day, hour, minute, second = map(int, m.groups())
    if second != 0:
        raise LineError(f"minute_utc 必须是整分钟（秒=0）: {value!r}")
    try:
        dt = datetime(year, month, day, hour, minute, 0, tzinfo=timezone.utc)
    except ValueError as exc:
        raise LineError(f"minute_utc 不是合法时间: {value!r} ({exc})") from exc
    return dt


def _strict_int(value: Any, field: str) -> int:
    # bool 是 int 的子类，需显式拒绝
    if isinstance(value, bool) or not isinstance(value, int):
        raise LineError(f"{field} 必须是整数")
    return value


def parse_line(text: str, source_file: str, source_line: int) -> Event:
    """解析并严格校验一行 JSONL，失败抛 LineError。"""
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise LineError(f"不是合法 JSON: {exc.msg} (col {exc.colno})") from exc

    if not isinstance(raw, dict):
        raise LineError("顶层必须是 JSON 对象")

    unknown = set(raw) - _KNOWN_FIELDS
    if unknown:
        raise LineError(f"包含未知字段: {sorted(unknown)}")

    missing = [f for f in _REQUIRED if f not in raw]
    if missing:
        raise LineError(f"缺少必填字段: {missing}")

    event_id = raw["event_id"]
    meter_id = raw["meter_id"]
    if not isinstance(event_id, str) or not event_id.strip():
        raise LineError("event_id 必须是非空字符串")
    if not isinstance(meter_id, str) or not meter_id.strip():
        raise LineError("meter_id 必须是非空字符串")

    revision = _strict_int(raw["revision"], "revision")
    if revision < 1:
        raise LineError("revision 必须是正整数")

    op = raw["op"]
    if op not in VALID_OPS:
        raise LineError(f"op 只允许 {VALID_OPS}，实际 {op!r}")

    minute_utc = _parse_minute(raw["minute_utc"])

    if op == "upsert":
        if "wh" not in raw:
            raise LineError("upsert 操作必须包含 wh")
        wh = _strict_int(raw["wh"], "wh")
        if wh < 0:
            raise LineError("wh 必须是非负整数")
    else:  # delete
        if "wh" in raw:
            raise LineError("delete 操作不允许携带 wh")
        wh = None

    return Event(
        event_id=event_id,
        meter_id=meter_id,
        minute_utc=minute_utc,
        revision=revision,
        op=op,
        wh=wh,
        payload_sha256=payload_hash(raw),
        source_file=source_file,
        source_line=source_line,
    )
