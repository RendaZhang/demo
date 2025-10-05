package com.example.demo.tracking;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Production-oriented Tracking 事件处理骨架（简化实现）。
 * <p>
 * 关键工程点（真实环境的做法）：
 * 1) 幂等：优先用数据库唯一键（events.carrier_event_id UNIQUE）或 Redis SETNX+TTL。
 * 2) 并发与顺序：以 shipmentId 分片（partition）串行处理，避免跨线程乱序导致的“回退”。
 * 3) 状态机：显式的允许迁移表，非法迁移直接拒绝或入审计表。
 * 4) 乱序：以 ts 为主序；ts 相等用 LWW（按到达顺序覆盖）；ts 更小的事件仅入 events/audit，不回滚主状态。
 * 5) 存储：Shipments（当前聚合状态）与 Events（事件溯源）分表；写主状态用 UPSERT/CAS。
 */
public final class TrackingIngestion {

    /* ========================== 领域模型 ========================== */

    public enum Status {
        CREATED, IN_TRANSIT, OUT_FOR_DELIVERY, DELIVERED, EXCEPTION
    }

    public static final class Event {
        public final String eventId;      // 承运商/平台事件唯一键 -> 幂等依据
        public final String shipmentId;   // 票件 ID -> 分片与读写主键
        public final long ts;             // 事件时间（毫秒）
        public final Status status;       // 业务状态
        public final String raw;          // 原始载荷（可选，方便审计/回放）
        public Event(String eventId, String shipmentId, long ts, Status status, String raw) {
            this.eventId = eventId;
            this.shipmentId = shipmentId;
            this.ts = ts;
            this.status = status;
            this.raw = raw;
        }
    }

    /** Shipments 表中的“当前聚合状态”（读路径主要依赖它）。 */
    public static final class ShipmentState {
        public final String shipmentId;
        public final Status status;
        public final long ts;
        public final long version;
        public ShipmentState(String shipmentId, Status status, long ts, long version) {
            this.shipmentId = shipmentId;
            this.status = status;
            this.ts = ts;
            this.version = version;
        }
        public ShipmentState with(Status status, long newTs) {
            return new ShipmentState(shipmentId, status, newTs, version + 1);
        }
        @Override
        public String toString() {
            return shipmentId + " -> (" + status + "," + ts + "), v" + version;
        }
    }

    /* ========================== 状态机与策略 ========================== */

    /**
     * 显式状态机：只允许白名单迁移，避免“状态倒退”。
     * 注意：某些业务允许 EXCEPTION -> IN_TRANSIT（异常解除），可按需配置。
     */
    public static final class StateMachine {
        private final Map<Status, Set<Status>> allow = new EnumMap<>(Status.class);
        private final boolean allowSame; // 是否允许相同状态覆盖（幂等/重复）
        public StateMachine(boolean allowSame) {
            this.allowSame = allowSame;
            for (Status s : Status.values()) {
                allow.put(s, EnumSet.noneOf(Status.class));
            }
            // 基本迁移（可按真实承运商语义调整/扩展）
            allow.get(Status.CREATED).addAll(EnumSet.of(Status.IN_TRANSIT, Status.OUT_FOR_DELIVERY, Status.DELIVERED, Status.EXCEPTION));
            allow.get(Status.IN_TRANSIT).addAll(EnumSet.of(Status.OUT_FOR_DELIVERY, Status.DELIVERED, Status.EXCEPTION));
            allow.get(Status.OUT_FOR_DELIVERY).addAll(EnumSet.of(Status.DELIVERED, Status.EXCEPTION));
            allow.get(Status.DELIVERED).add(Status.DELIVERED); // 交付后仅允许幂等重复
            allow.get(Status.EXCEPTION).addAll(EnumSet.of(Status.IN_TRANSIT, Status.OUT_FOR_DELIVERY, Status.DELIVERED, Status.EXCEPTION));
        }
        public boolean canTransition(Status from, Status to) {
            if (from == to) {
                return true;
            }
            return allow.get(from).contains(to);
        }
    }

    /* ========================== 存储抽象（真实用 DB/Redis） ========================== */

    /** 幂等存储：生产建议用 DB UNIQUE 或 Redis SETNX + EXPIRE（重放窗如 5~10 分钟）。 */
    public interface IdempotencyStore {
        boolean firstSeen(String eventId, long nowMillis, long ttlMillis);
    }

    public static final class InMemoryIdempotencyStore implements IdempotencyStore {
        private final ConcurrentHashMap<String, Long> exp = new ConcurrentHashMap<>();
        @Override
        public boolean firstSeen(String eventId, long now, long ttl) {
            Long newExp = now + ttl;
            Long old = exp.putIfAbsent(eventId, newExp);
            if (old == null) return true; // 首次出现
            if (old < now) {              // 过期重放窗外 -> 允许刷新
                exp.replace(eventId, newExp);
                return true;
            }
            return false;                 // 窗口内重复 -> 丢弃
        }
        // 可增加定时清理线程以控制内存；生产用 Redis/DB 则无需本地清理
    }

    /** Shipments 与 Events 的存取（真实应拆两表，并保证 events.eventId 唯一）。 */
    public interface ShipmentRepository {
        ShipmentState getShipment(String shipmentId);
        void upsertShipment(ShipmentState newState);   // 生产用 UPSERT 或乐观锁（where version=?）
        void appendEvent(Event e);                     // 生产用 INSERT IGNORE/ON CONFLICT，基于 eventId 去重
    }

    public static final class InMemoryShipmentRepository implements ShipmentRepository {
        private final ConcurrentHashMap<String, ShipmentState> shipments = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Event> events = new ConcurrentHashMap<>();  // eventId -> Event
        @Override
        public ShipmentState getShipment(String shipmentId) {
            return shipments.get(shipmentId);
        }
        @Override
        public void upsertShipment(ShipmentState newState) {
            shipments.put(newState.shipmentId, newState);
        }
        @Override
        public void appendEvent(Event e) {
            events.putIfAbsent(e.eventId, e);
        }
    }

    /* ========================== 并发控制（按 key 串行） ========================== */

    /** 简单分片锁：同一 shipmentId 命中同一把锁，保证处理顺序与原子性。 */
    public static final class StripedLocks {
        private final ReentrantLock[] locks;
        public StripedLocks(int stripes) {
            locks = new ReentrantLock[stripes];
            for (int i = 0; i < stripes; i++) {
                locks[i] = new ReentrantLock();
            }
        }
        public ReentrantLock lockFor(String key) {
            int idx = Math.floorMod(key.hashCode(), locks.length);
            return locks[idx];
        }
    }

    /* ========================== 核心处理器 ========================== */

    public static final class Processor {
        private final ShipmentRepository repo;
        private final IdempotencyStore idem;
        private final StateMachine fsm;
        private final StripedLocks locks;
        private final long idemTtlMs;  // 重放窗口，如 10 分钟
        public Processor(ShipmentRepository repo, IdempotencyStore idem, StateMachine fsm, int stripes, long idemTtlMs) {
            this.repo = repo;
            this.idem = idem;
            this.fsm = fsm;
            this.locks = new StripedLocks(stripes);
            this.idemTtlMs = idemTtlMs;
        }
        /**
         * 处理单个事件（线程安全，按 shipmentId 串行）。
         * 规则：
         *  - 幂等：窗口内重复 eventId 直接丢弃；
         *  - 主状态只接受 ts 更大的事件；ts 相等采用 LWW（覆盖）；
         *  - ts 更小的不回滚主状态，但会入 events（便于审计/回放）；
         *  - 状态迁移必须符合状态机；不符合则仅审计，不更新主状态。
         */
        public void ingest(Event e) {
            long now = System.currentTimeMillis();
            if (!idem.firstSeen(e.eventId, now, idemTtlMs)) {
                // 窗口内重复，直接返回；生产中仍可 appendEvent 计数/审计
                return;
            }
            // 记录原始事件（生产环境应基于 eventId 唯一键，失败即说明重复）
            repo.appendEvent(e);
            // 开始线程安全的处理
            ReentrantLock lock = locks.lockFor(e.shipmentId);
            lock.lock();
            try {
                ShipmentState cur = repo.getShipment(e.shipmentId);
                if (cur == null) {
                    // 首事件：直接落主状态
                    repo.upsertShipment(new ShipmentState(e.shipmentId, e.status, e.ts, 1));
                    return;
                }
                if (e.ts > cur.ts) {
                    // 新事件时间更晚 -> 需要通过状态机校验
                    if (fsm.canTransition(cur.status, e.status)) {
                        repo.upsertShipment(cur.with(e.status, e.ts));
                    } else {
                        // 非法迁移：仅审计，不更新主状态（可打告警）
                        // 生产：写入 audit_log 表 or dead_letter，触发告警/人工回查
                    }
                } else if (e.ts == cur.ts) {
                    // 同 ts -> LWW（题目指定：按到达顺序覆盖）
                    if (fsm.canTransition(cur.status, e.status) || cur.status == e.status) {
                        repo.upsertShipment(cur.with(e.status, e.ts));
                    } else {
                        // 同 ts 但非法迁移：按策略可选择忽略或审计
                    }
                } else {
                    // e.ts < cur.ts：早到/迟来的旧事件，只审计不回滚
                    // 生产：也可做“有界延迟重算”
                    // 有界延迟重算：
                    // - 对同一 shipment 维护一个短窗口缓冲（例如 2–5 分钟水位）。
                    // - 窗口内按 ts 排序再归并，窗口外再更新主状态，降低乱序影响；
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /* ========================== Demo ========================== */

    public static void main(String[] args) {
        ShipmentRepository repo = new InMemoryShipmentRepository();
        IdempotencyStore idem = new InMemoryIdempotencyStore();
        StateMachine fsm = new StateMachine(true);
        // 初始设置：根据经验，条纹数量可以设置为 CPU 核心数的 2 倍。例如，如果 CPU 有 16 个核心，条纹数量可以设置为 32。
        // 然后根据性能测试结果更新设置条纹数量
        Processor p = new Processor(repo, idem, fsm, /*stripes*/ 64, /*idemTtlMs*/ 10 * 60_000);
        // 示例序列：包含重复、乱序、非法迁移等
        List<Event> input = List.of(
                new Event("e1","S1",1000, Status.IN_TRANSIT, null),
                new Event("e2","S1",1200, Status.OUT_FOR_DELIVERY, null),
                new Event("e3","S2", 900, Status.CREATED, null),
                new Event("e1","S1",1000, Status.IN_TRANSIT, null),  // 重复，丢弃
                new Event("e4","S1", 800, Status.CREATED, null),     // 旧事件，仅审计
                new Event("e5","S2",1100, Status.DELIVERED, null),   // 合法迁移
                new Event("e6","S1",1300, Status.IN_TRANSIT, null)   // 非法回退：OFD -> IN_TRANSIT，被拦截
        );
        for (Event e : input) {
            p.ingest(e);
        }
        System.out.println(repo.getShipment("S1"));  // 期望：S1 -> (OUT_FOR_DELIVERY,1200), v2
        System.out.println(repo.getShipment("S2"));  // 期望：S2 -> (DELIVERED,1100), v1
    }
}
