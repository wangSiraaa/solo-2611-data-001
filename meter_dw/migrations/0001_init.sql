-- 0001_init.sql
-- 电表分钟数据增量入仓：事件溯源、版本化状态、冲突隔离、坏行登记、
-- 文件检查点、任务运行记录、北京时间日报。
-- 目标：PostgreSQL 16。

BEGIN;

CREATE SCHEMA IF NOT EXISTS meter;

-- 迁移版本表 ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meter.schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 已接受的业务事件（事件溯源：日报可回溯到原始事件） -----------------------
-- 同一 event_id 只允许一条有效记录；冲突记录进隔离表，不进入本表。
CREATE TABLE meter.events (
    event_id        TEXT PRIMARY KEY,
    meter_id        TEXT NOT NULL,
    minute_utc      TIMESTAMPTZ NOT NULL,
    revision        BIGINT NOT NULL CHECK (revision >= 1),
    op              TEXT NOT NULL CHECK (op IN ('upsert', 'delete')),
    wh              BIGINT CHECK (wh IS NULL OR wh >= 0),
    payload_sha256  TEXT NOT NULL,
    source_file     TEXT NOT NULL,
    source_line     INTEGER NOT NULL CHECK (source_line > 0),
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT events_wh_rule CHECK (
        (op = 'upsert' AND wh IS NOT NULL)
        OR (op = 'delete' AND wh IS NULL)
    )
);
COMMENT ON TABLE meter.events IS '首次有效处理的业务事件原文索引，用于历史日报追溯';

-- 每块电表每分钟的当前有效版本（版本化，防止旧版重放复活） -----------------
CREATE TABLE meter.meter_minute_state (
    meter_id        TEXT NOT NULL,
    minute_utc      TIMESTAMPTZ NOT NULL,
    revision        BIGINT NOT NULL,
    op              TEXT NOT NULL CHECK (op IN ('upsert', 'delete')),
    wh              BIGINT,
    event_id        TEXT NOT NULL REFERENCES meter.events(event_id),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT meter_minute_state_pk PRIMARY KEY (meter_id, minute_utc)
);
COMMENT ON TABLE meter.meter_minute_state IS '同表同分钟只保留最高修订号的有效记录；删除以墓碑行保留版本';

-- 隔离记录：事件内容冲突、修订冲突、过期重放等 -----------------------------
CREATE TABLE meter.quarantine (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reason_code     TEXT NOT NULL CHECK (reason_code IN (
                        'event_id_content_conflict',
                        'revision_conflict',
                        'stale_revision_replay'
                    )),
    reason_detail   TEXT NOT NULL,
    event_id        TEXT,
    meter_id        TEXT,
    minute_utc      TIMESTAMPTZ,
    revision        BIGINT,
    op              TEXT,
    wh              BIGINT,
    payload_sha256  TEXT NOT NULL,
    source_file     TEXT NOT NULL,
    source_line     INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE meter.quarantine IS '业务语义冲突行：保留首次有效记录，冲突行隔离并注明原因';

-- 格式错误行：记录文件与行号，不阻断后续有效数据 ---------------------------
CREATE TABLE meter.bad_lines (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file     TEXT NOT NULL,
    source_line     INTEGER NOT NULL,
    raw_text        TEXT NOT NULL,
    reason_detail   TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 入仓文件与检查点 ----------------------------------------------------------
CREATE TABLE meter.ingest_files (
    file_name       TEXT PRIMARY KEY,
    source_path     TEXT NOT NULL,
    sha256          TEXT NOT NULL,
    byte_size       BIGINT NOT NULL,
    line_count      INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('processing', 'done')),
    last_line       INTEGER NOT NULL DEFAULT 0,
    batches         INTEGER NOT NULL DEFAULT 0,
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);
COMMENT ON TABLE meter.ingest_files IS '文件级检查点；续跑前必须重新计算 sha256 并与此处比对';

-- 任务运行记录 --------------------------------------------------------------
CREATE TABLE meter.task_runs (
    run_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_type       TEXT NOT NULL CHECK (task_type IN ('import', 'resume')),
    status          TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    files_requested INTEGER,
    files_done      INTEGER NOT NULL DEFAULT 0,
    lines_seen      BIGINT NOT NULL DEFAULT 0,
    events_accepted BIGINT NOT NULL DEFAULT 0,
    quarantined     BIGINT NOT NULL DEFAULT 0,
    bad_lines       BIGINT NOT NULL DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

-- 北京时间自然日日报（增量维护，只更新受影响日期） -------------------------
CREATE TABLE meter.daily_report (
    meter_id            TEXT NOT NULL,
    report_date         DATE NOT NULL,            -- 北京时间自然日
    valid_minutes       BIGINT NOT NULL DEFAULT 0,
    total_wh            BIGINT NOT NULL DEFAULT 0,
    last_event_id       TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT daily_report_pk PRIMARY KEY (meter_id, report_date)
);
COMMENT ON TABLE meter.daily_report IS '按 Asia/Shanghai 自然日汇总有效 upsert 分钟；delete 墓碑不计入';

-- 索引 ----------------------------------------------------------------------
CREATE INDEX events_meter_minute_idx
    ON meter.events (meter_id, minute_utc);
CREATE INDEX state_meter_date_idx
    ON meter.meter_minute_state (meter_id, ((minute_utc AT TIME ZONE 'Asia/Shanghai')::date));
CREATE INDEX quarantine_file_idx
    ON meter.quarantine (source_file, source_line);
CREATE INDEX bad_lines_file_idx
    ON meter.bad_lines (source_file, source_line);
CREATE INDEX task_runs_started_idx
    ON meter.task_runs (started_at DESC);

-- 日报重算函数：输入 (电表, 北京时间日期)，从当前状态全量重算该格 ----------
-- 只重算受影响的单个日期，不做全表重算。该格没有任何有效 upsert 分钟时
-- （例如分钟全部被 delete 墓碑取代）删除报告行，避免留下 0 值幽灵格。
CREATE OR REPLACE FUNCTION meter.recompute_daily(
    p_meter_id   TEXT,
    p_report_date DATE
) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_minutes BIGINT;
BEGIN
    INSERT INTO meter.daily_report (meter_id, report_date, valid_minutes, total_wh,
                                    last_event_id, updated_at)
    SELECT s.meter_id,
           p_report_date,
           count(*) FILTER (WHERE s.op = 'upsert'),
           coalesce(sum(s.wh) FILTER (WHERE s.op = 'upsert'), 0),
           max(s.event_id),
           now()
      FROM meter.meter_minute_state s
     WHERE s.meter_id = p_meter_id
       AND (s.minute_utc AT TIME ZONE 'Asia/Shanghai')::date = p_report_date
    GROUP BY s.meter_id
    ON CONFLICT (meter_id, report_date) DO UPDATE
       SET valid_minutes = EXCLUDED.valid_minutes,
           total_wh     = EXCLUDED.total_wh,
           last_event_id= EXCLUDED.last_event_id,
           updated_at   = now();

    SELECT valid_minutes INTO v_minutes
      FROM meter.daily_report
     WHERE meter_id = p_meter_id AND report_date = p_report_date;

    IF v_minutes IS NOT NULL AND v_minutes = 0 THEN
        DELETE FROM meter.daily_report
         WHERE meter_id = p_meter_id AND report_date = p_report_date;
    END IF;
END;
$$;
COMMENT ON FUNCTION meter.recompute_daily IS '按电表+北京时间日期重算单格日报，删除墓碑不计入；全删时移除报告行';

INSERT INTO meter.schema_migrations (version) VALUES ('0001_init')
ON CONFLICT (version) DO NOTHING;

COMMIT;
