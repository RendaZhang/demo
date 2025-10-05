package com.example.demo.limiter;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class AdaptiveRateLimiter {

    public static final class RateConfig {
        public final double defaultRatePerSec; // 基础速率
        public final int defaultBurst;         // 突发上限（令牌数）
        public final long initialBackoffMs;    // 初始退避
        public final long maxBackoffMs;        // 最大退避
        public final long rampMs;              // 平滑恢复时长
        public RateConfig(double defaultRatePerSec, int defaultBurst, long initialBackoffMs, long maxBackoffMs, long rampMs) {
            this.defaultRatePerSec = defaultRatePerSec;
            this.defaultBurst = defaultBurst;
            this.initialBackoffMs = initialBackoffMs;
            this.maxBackoffMs = maxBackoffMs;
            this.rampMs = rampMs;
        }
    }

    private static final class Key {
        final String tenant, carrier;
        Key(String t, String c) {tenant = t; carrier = c;}
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key)) return false;
            Key k = (Key) o;
            return Objects.equals(tenant, k.tenant) && Objects.equals(carrier, k.carrier);
        }
        @Override
        public int hashCode() {
            return Objects.hash(tenant, carrier);
        }
    }

    private static final class Bucket {
        final ReentrantLock lock = new ReentrantLock();
        final RateConfig cfg;
        // 配置（可后续做成可变）
        double baseRatePerSec;
        int baseBurst;
        // 令牌桶状态
        double tokens;
        long lastRefillNanos;
        // 自适应退避状态
        int backoffExp = 0;
        long backoffUntilNanos = 0L;   // 冷却截止
        long rampUntilNanos = 0L;      // 平滑恢复截止

        Bucket(RateConfig cfg) {
            this.cfg = cfg;
            this.baseRatePerSec = cfg.defaultRatePerSec;
            this.baseBurst = cfg.defaultBurst;
            this.tokens = baseBurst;   // 冷启动允许突发
            this.lastRefillNanos = System.nanoTime();
        }

        boolean tryAcquire(int permits) {
            final long now = System.nanoTime();
            lock.lock();
            try {
                // 冷却期：一律拒绝，冻结补充（避免冷却结束瞬间堆满）
                if (now < backoffUntilNanos) {
                    lastRefillNanos = now;
                    return false;
                }
                // 计算平滑恢复系数 ramp ∈ (0,1]：冷却结束到 ramp 结束之间线性上升
                double ramp = 1.0;
                if (now < rampUntilNanos) {
                    long span = Math.max(1, rampUntilNanos - backoffUntilNanos);
                    ramp = Math.max(0.0, Math.min(1.0, (double)(now - backoffUntilNanos) / span));
                }
                // 按有效速率与容量补充（随 ramp 缩放）
                double effRate = baseRatePerSec * ramp;
                double effBurst = baseBurst * ramp;
                long elapsed = now - lastRefillNanos;
                if (elapsed > 0) {
                    double add = effRate * (elapsed / 1_000_000_000.0);
                    tokens = Math.min(effBurst, tokens + add);
                    lastRefillNanos = now;
                }
                if (tokens >= permits) {
                    tokens -= permits;
                    // 若已完全恢复，则逐步复位退避指数
                    if (now >= rampUntilNanos && backoffExp > 0) backoffExp = 0;
                    return true;
                } else {
                    return false;
                }
            } finally {
                lock.unlock();
            }
        }

        void on429() {
            final long now = System.nanoTime();
            lock.lock();
            try {
                // 计算新的退避长度（指数增加，封顶）
                long backoffMs = Math.min(
                        cfg.maxBackoffMs,
                        cfg.initialBackoffMs << Math.min(backoffExp, 16) // 防止位移溢出
                );
                backoffExp = Math.min(backoffExp + 1, 16);
                backoffUntilNanos = now + TimeUnit.MILLISECONDS.toNanos(backoffMs);
                // 平滑恢复期，固定用 rampMs（也可按 backoffMs 比例放大）
                rampUntilNanos = backoffUntilNanos + TimeUnit.MILLISECONDS.toNanos(cfg.rampMs);
                // 冷却期冻结并清空，避免冷却结束瞬间超发
                tokens = 0.0;
                lastRefillNanos = now;
            } finally {
                lock.unlock();
            }
        }
    }

    private final RateConfig cfg;
    private final ConcurrentHashMap<Key, Bucket> buckets = new ConcurrentHashMap<>();

    public AdaptiveRateLimiter(RateConfig cfg) {
        this.cfg = cfg;
    }

    private Bucket bucketOf(String tenantId, String carrier) {
        Key k = new Key(tenantId, carrier);
        return buckets.computeIfAbsent(k, _k -> new Bucket(cfg));
    }

    // --- 公共接口 ---
    public boolean tryAcquire(String tenantId, String carrier, int permits) {
        if (permits <= 0) return true;
        return bucketOf(tenantId, carrier).tryAcquire(permits);
    }

    public void on429(String tenantId, String carrier) {
        bucketOf(tenantId, carrier).on429();
    }

    // 示例：创建一个缺省 limiter
    public static AdaptiveRateLimiter defaultLimiter() {
        return new AdaptiveRateLimiter(new RateConfig(
                100.0,   // 100 req/s
                200,     // 200 突发
                200,     // 初始退避 200ms
                10_000,  // 最大退避 10s
                1_000    // 平滑恢复 1s
        ));
    }
}
