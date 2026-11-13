"""合成 JSONL 数据生成器。

生成三个不可变输入批次，覆盖题面要求的全部场景：
- batch1_midnight.jsonl: 跨北京时间零点归属、重复投递、同事件内容冲突、
  同修订号业务冲突、坏行混入；
- batch2_nextday.jsonl:  次日修订前日用电（upsert 修正 + delete 修订）；
- batch3_replay.jsonl:   删除后重放旧版（必须被隔离、不能复活）。

另生成 tampered.jsonl：导入后再篡改其字节，用于验证续跑摘要拒绝逻辑。
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent.parent / "data" / "incoming"


def line(event_id: str, meter: str, ts: str, rev: int, op: str,
         wh: int | None = None) -> str:
    obj: dict = {
        "event_id": event_id,
        "meter_id": meter,
        "minute_utc": ts,
        "revision": rev,
        "op": op,
    }
    if wh is not None:
        obj["wh"] = wh
    return json.dumps(obj, ensure_ascii=False)


def write(name: str, rows: list[str]) -> Path:
    path = OUT / name
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    # ---- 批次 1：跨零点 + 重复/冲突/坏行 --------------------------------
    b1: list[str] = [
        # M1：15:59/16:00/16:01 UTC（北京 23:59 / 次日 00:00 / 00:01）
        line("e-m1-01", "M1", "2026-11-12T15:59:00Z", 1, "upsert", 100),
        line("e-m1-02", "M1", "2026-11-12T16:00:00Z", 1, "upsert", 110),
        line("e-m1-03", "M1", "2026-11-12T16:01:00Z", 1, "upsert", 120),
        # 同事件重复投递（内容完全一致）——跳过，不重复处理
        line("e-m1-02", "M1", "2026-11-12T16:00:00Z", 1, "upsert", 110),
        # 同一 event_id 内容不同——隔离 event_id_content_conflict
        line("e-m1-03", "M1", "2026-11-12T16:01:00Z", 1, "upsert", 999),
        # 同修订号业务内容冲突（分钟 16:01，rev 1 已存在且 wh=120）——隔离
        line("e-m1-04", "M1", "2026-11-12T16:01:00Z", 1, "upsert", 121),
        # 同修订号同内容的另一事件（分钟 16:00，rev1 wh=110）——接受留痕，状态不变
        line("e-m1-05", "M1", "2026-11-12T16:00:00Z", 1, "upsert", 110),
        # 高修订号覆盖 16:00：wh 110 -> 115
        line("e-m1-06", "M1", "2026-11-12T16:00:00Z", 2, "upsert", 115),
        # 旧版本晚到（rev1 试图覆盖 rev2）——隔离 stale_revision_replay
        line("e-m1-07", "M1", "2026-11-12T16:00:00Z", 1, "upsert", 110),
        # M2：两个有效 upsert 分钟，随后删除其一（墓碑保留版本）
        line("e-m2-01", "M2", "2026-11-12T16:00:00Z", 1, "upsert", 200),
        line("e-m2-02", "M2", "2026-11-12T16:01:00Z", 1, "upsert", 210),
        line("e-m2-03", "M2", "2026-11-12T16:01:00Z", 2, "delete"),
        # 坏行：JSON 语法错误（带文件名行号登记，后续行照常入仓）
        '{"event_id": "bad-1", "meter_id": "M1", broken json',
        # 坏行：缺少 wh
        json.dumps({"event_id": "bad-2", "meter_id": "M1",
                    "minute_utc": "2026-11-12T16:02:00Z",
                    "revision": 1, "op": "upsert"}),
        # 坏行：未知字段
        line("bad-3", "M1", "2026-11-12T16:03:00Z", 1, "upsert", 50)[:-1]
        + ', "extra": 1}',
        # 坏行：非整分钟
        line("bad-4", "M1", "2026-11-12T16:04:30Z", 1, "upsert", 50),
        # 坏行：revision 非正整数 / wh 负数 / delete 带 wh
        line("bad-5", "M1", "2026-11-12T16:05:00Z", 0, "upsert", 50),
        line("bad-6", "M1", "2026-11-12T16:06:00Z", 1, "upsert", -1),
        line("bad-7", "M1", "2026-11-12T16:07:00Z", 1, "delete", 50),
        # 坏行之后仍有有效数据——证明坏行不阻断
        line("e-m2-04", "M2", "2026-11-12T16:02:00Z", 1, "upsert", 220),
    ]
    write("batch1_midnight.jsonl", b1)

    # ---- 批次 2：次日到达的计量修正 --------------------------------------
    b2: list[str] = [
        # 修正 M1 北京 11-12 的 15:59 分钟：100 -> 105（rev2 覆盖 rev1）
        line("e-m1-08", "M1", "2026-11-12T15:59:00Z", 2, "upsert", 105),
        # 删除 M2 的 16:00 分钟（rev2 墓碑）——北京 11-13 该分钟不再计入
        line("e-m2-05", "M2", "2026-11-12T16:00:00Z", 2, "delete"),
        # 新一天的数据：北京 11-13
        line("e-m1-09", "M1", "2026-11-12T16:10:00Z", 1, "upsert", 300),
        line("e-m3-01", "M3", "2026-11-12T16:10:00Z", 1, "upsert", 400),
    ]
    write("batch2_nextday.jsonl", b2)

    # ---- 批次 3：删除后重放旧版 ------------------------------------------
    b3: list[str] = [
        # M1 16:00 当前为 rev2 wh=115，重放 rev1——隔离，值保持 115
        line("e-m1-10", "M1", "2026-11-12T16:00:00Z", 1, "upsert", 110),
        # M2 16:01 当前为 rev2 delete 墓碑，重放 rev1 upsert——隔离，删除不复活
        line("e-m2-06", "M2", "2026-11-12T16:01:00Z", 1, "upsert", 210),
        # M2 16:00 当前为 rev2 delete 墓碑，重放 rev1 upsert——同样隔离
        line("e-m2-07", "M2", "2026-11-12T16:00:00Z", 1, "upsert", 200),
        # 同事件再投（e-m2-05 已在批次2处理）——重复跳过
        line("e-m2-05", "M2", "2026-11-12T16:00:00Z", 2, "delete"),
        # 合法新数据
        line("e-m3-02", "M3", "2026-11-12T16:11:00Z", 1, "upsert", 410),
    ]
    write("batch3_replay.jsonl", b3)

    # ---- 篡改验证文件（脚本运行后由验证脚本改写其字节） ------------------
    write("tampered.jsonl", [
        line("e-t-01", "M9", "2026-11-12T16:20:00Z", 1, "upsert", 10),
        line("e-t-02", "M9", "2026-11-12T16:21:00Z", 1, "upsert", 20),
    ])

    for p in sorted(OUT.glob("*.jsonl")):
        print(f"generated {p.name} ({sum(1 for _ in p.open())} lines)")


if __name__ == "__main__":
    main()
