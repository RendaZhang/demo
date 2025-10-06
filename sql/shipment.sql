-- PostgreSQL 14+

-- 概览表：每票件一行
CREATE TABLE shipments (
  id                    BIGSERIAL       PRIMARY KEY,
  tenant_id             BIGINT          NOT NULL,
  tracking_no           TEXT            NOT NULL,
  carrier               TEXT            NOT NULL,
  last_status           TEXT            NOT NULL,
  last_checkpoint_at   TIMESTAMPTZ     NOT NULL,
  updated_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
  version               INT             NOT NULL DEFAULT 0
);

-- 查询目标驱动建模：Q1 需要 ≤10ms 返回“当前最新状态 + 最后 checkpoint”，
-- 所以把这两项做成 shipments 的派生列，由摄取链路在写入 events 时条件更新，避免每次查询再扫明细。

-- 唯一 + 覆盖索引，便于 Q1 做 index-only（取决于可见性图）
CREATE UNIQUE INDEX ux_shipments_tenant_tracking
on shipments(tenant_id, tracking_no)
INCLUDE (last_status, last_checkpoint_at);

-- 事件明细表：一条轨迹一个事件
CREATE TABLE events (
  id                    BIGSERIAL       PRIMARY KEY,
  tenant_id             BIGINT          NOT NULL,
  shipment_id           BIGINT          NOT NULL REFERENCES shipments(id),
  carrier_event_id      TEXT            NOT NULL,
  ts                    TIMESTAMPTZ     NOT NULL,
  status                TEXT            NOT NULL,
  location              JSONB,
  raw                   JSONB
);

-- 事件去重（多租户维度）
CREATE UNIQUE INDEX ux_events_tenant_carrier_event
ON events(tenant_id, carrier_event_id);

-- 去重：(tenant_id, carrier_event_id) 唯一约束，插入冲突即视为重复。

-- 游标分页：events 以 (shipment_id, ts desc, id desc) 建复合索引，
-- 配合 seek 模式 (ts, id) < (cursor_ts, cursor_id)，完全避免 OFFSET。

-- 游标分页所需排序索引（倒序）
CREATE INDEX ix_events_shipment_ts_id_desc
ON events(shipment_id, ts DESC, id DESC);
-- timestamptz 统一时区处理；倒序索引与查询排序一致，避免额外排序。

-- Q1：已知 (tenant_id, tracking_no)，在 ≤10ms 内查询“当前最新状态 + 最后 checkpoint 时间”。
SELECT last_status AS status, last_checkpoint_at
FROM shipments
WHERE tenant_id = $1 AND tracking_no = $2;
-- 命中 ux_shipments_tenant_tracking，理想情况下 index-only scan。

-- Q2：给定 shipment_id，按时间倒序分页返回事件时间线，要求游标分页（避免 OFFSET），游标由上一次返回的 (ts, id) 组成。
-- 首页（无游标）
SELECT id, ts, status, location
FROM events
WHERE shipment_id = $1
ORDER BY ts DESC, id DESC
LIMIT $2;
-- 后续页（携带游标 cursor_ts, cursor_id）
SELECT id, ts, status, location
FROM events
WHERE shipment_id = $1
  AND (ts, id) < ($2, $3)            -- row-wise 比较：严格小于上一页最后一条
ORDER BY ts DESC, id DESC
LIMIT $4;
-- 去重插入（重复忽略）
INSERT INTO events (tenant_id, shipment_id, carrier_event_id, ts, status, location, raw)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, carrier_event_id) DO NOTHING;
