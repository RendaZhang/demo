package com.example.demo;

import java.util.*;

public class TrackingAggregator {

    public enum Status {
        CREATED, IN_TRANSIT, OUT_FOR_DELIVERY, DELIVERED, EXCEPTION
    }

    public static final class Event {
        public final String eventID;
        private final String shipmentID;
        private final long ts;
        private final Status status;

        public Event(String eventID, String shipmentID, long ts, Status status) {
            this.eventID = Objects.requireNonNull(eventID);
            this.shipmentID = Objects.requireNonNull(shipmentID);
            this.ts = ts;
            this.status = Objects.requireNonNull(status);
        }
    }

    public static final class ShipmentState {
        public final Status status;
        public final long ts;

        public ShipmentState(Status status, long ts) {
            this.status = Objects.requireNonNull(status);
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "(" + status + ", " + ts + ")";
        }
    }

    public static Map<String, ShipmentState> summarizeLatest(List<Event> events) {
        Map<String, ShipmentState> latest = new HashMap<>();
        Set<String> seenEventIds = new HashSet<>(Math.max(16, events.size()));
        for (Event event : events) {
            if (!seenEventIds.add(event.eventID)) continue;
            ShipmentState curr = latest.get(event.shipmentID);
            if (curr == null || event.ts > curr.ts || event.ts == curr.ts) {
                latest.put(event.shipmentID, new ShipmentState(event.status, event.ts));
            }
        }
        return latest;
    }

    public static void main(String[] args) {
        List<Event> input = Arrays.asList(
                new Event("e1","S1",1000, Status.IN_TRANSIT),
                new Event("e2","S1",1200, Status.OUT_FOR_DELIVERY),
                new Event("e3","S2", 900, Status.CREATED),
                new Event("e1","S1",1000, Status.IN_TRANSIT), // 重复，需丢弃
                new Event("e4","S1", 800, Status.CREATED),    // 更早的乱序，忽略
                new Event("e5","S2",1100, Status.DELIVERED)
        );
        Map<String, ShipmentState> out = summarizeLatest(input);
        System.out.println("S1 -> " + out.get("S1"));
        System.out.println("S2 -> " + out.get("S2"));
    }

}
