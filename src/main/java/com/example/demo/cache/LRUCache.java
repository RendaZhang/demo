package com.example.demo.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * 实现 LRU 缓存（不允许用 LinkedHashMap 的内置淘汰）**
 * 请用 Java 实现一个 LRU 缓存，要求：
 * - get(key)：存在返回 value，并将该键提升为最近使用；不存在返回 -1。
 * - put(key, value)：写入或更新，并将该键提升为最近使用；容量满时淘汰最久未使用的键。
 * - 期望复杂度：get/put 都是 O(1)。
 * - 不能依赖 LinkedHashMap 的 removeEldestEntry 等内置淘汰机制；你可以自己实现“双向链表 + HashMap”。
 * 补充说明与边界：
 * - capacity 可能为 1；如为 0，get 永远 -1，put 不存储。
 * - put 更新已有 key 时，应该只更新值并移动到最近使用。
 * - 请写出完整可编译代码，并简单解释你的数据结构与关键操作。
 */
public class LRUCache {

    // 下面用「哈希表 + 双向链表（带哨兵）」实现，get/put 都是 O(1)。容量为 0 时，put 直接忽略、get 恒为 -1。

    // 双向链表节点
    private static class Node {
        int key, value;
        Node prev, next;
        Node(int key, int value) {this.key = key; this.value = value;}
    }

    private final int capacity;
    private final Map<Integer, Node> map;
    // 头尾哨兵：head.next 是最近使用，tail.prev 是最久未使用
    private final Node head, tail;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.map = new HashMap<>();
        this.head = new Node(-1, -1);
        this.tail = new Node(-1, -1);
        head.next = tail;
        tail.prev = head;
    }

    public int get(int key) {
        Node node = map.get(key);
        if (node == null) return -1;
        // 提升为最近使用
        moveToFront(node);
        return node.value;
    }

    public void put(int key, int value) {
        if (capacity == 0) return;  // 容量为 0：不存储
        Node node = map.get(key);
        if (node != null) {
            node.value = value;     // 更新值
            moveToFront(node);      // 提升为最近使用
            return;
        }
        // 新键
        Node fresh = new Node(key, value);
        map.put(key, fresh);
        addAfterHead(fresh);
        // 如超过容量，淘汰最久未使用（链表尾部前一项）
        if (map.size() >= capacity) {
            Node lru = popTail();
            if (lru != null) map.remove(lru.key);
        }
    }

    // ---------- 双向链表原子操作（均为 O(1)） ----------

    // 将节点放到 head 之后（成为最近使用）
    private void addAfterHead(Node node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    // 从链表中摘除节点
    private void removeNode(Node node) {
        Node p = node.prev, n = node.next;
        p.next = n;
        n.prev = p;
        node.prev = node.next = null; // 可选：帮助 GC
    }

    // 把节点移动到最前面
    private void moveToFront(Node node) {
        removeNode(node);
        addAfterHead(node);
    }

    // 弹出最尾部的真实节点（最久未使用）
    private Node popTail() {
        Node lru = tail.prev;
        if (lru == head) return null; // 空链表保护
        removeNode(lru);
        return lru;
    }
}
