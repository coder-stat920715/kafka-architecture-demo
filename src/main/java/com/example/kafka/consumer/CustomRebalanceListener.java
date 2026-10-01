package com.example.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Logs and tracks partition assignment changes during consumer group rebalances.
 * With the CooperativeStickyAssignor, {@code assigned}/{@code revoked} contain only the partitions
 * that actually moved, so the tracked state is maintained incrementally.
 */
@Slf4j
@Component
public class CustomRebalanceListener implements ConsumerAwareRebalanceListener {

    private final Map<String, Set<TopicPartition>> assignmentsByConsumerThread = new ConcurrentHashMap<>();
    private final AtomicLong assignedEvents = new AtomicLong();
    private final AtomicLong revokedEvents = new AtomicLong();
    private final AtomicLong lostEvents = new AtomicLong();

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        revokedEvents.incrementAndGet();
        log.warn("[REBALANCE] thread={} member={} REVOKING (before offset commit): {}",
                Thread.currentThread().getName(), memberId(consumer), format(partitions));
    }

    @Override
    public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        assignmentsByConsumerThread
                .computeIfAbsent(Thread.currentThread().getName(), k -> ConcurrentHashMap.newKeySet())
                .removeAll(partitions);
        log.warn("[REBALANCE] thread={} REVOKED (offsets committed): {}",
                Thread.currentThread().getName(), format(partitions));
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        assignedEvents.incrementAndGet();
        assignmentsByConsumerThread
                .computeIfAbsent(Thread.currentThread().getName(), k -> ConcurrentHashMap.newKeySet())
                .addAll(partitions);
        log.info("[REBALANCE] thread={} member={} ASSIGNED: {}",
                Thread.currentThread().getName(), memberId(consumer), format(partitions));
        for (TopicPartition tp : partitions) {
            try {
                log.info("[REBALANCE]   {} will resume from position {}", tp, consumer.position(tp));
            } catch (Exception e) {
                log.debug("[REBALANCE]   position unavailable for {}: {}", tp, e.getMessage());
            }
        }
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        lostEvents.incrementAndGet();
        assignmentsByConsumerThread
                .computeIfAbsent(Thread.currentThread().getName(), k -> ConcurrentHashMap.newKeySet())
                .removeAll(partitions);
        log.error("[REBALANCE] thread={} LOST partitions (session expired / fenced): {}",
                Thread.currentThread().getName(), format(partitions));
    }

    /** Snapshot for the cluster-status endpoint. */
    public Map<String, Object> snapshot() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("assignedEvents", assignedEvents.get());
        result.put("revokedEvents", revokedEvents.get());
        result.put("lostEvents", lostEvents.get());
        Map<String, Object> assignments = new LinkedHashMap<>();
        assignmentsByConsumerThread.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> assignments.put(e.getKey(), e.getValue().stream()
                        .map(TopicPartition::toString)
                        .sorted()
                        .toList()));
        result.put("currentAssignmentsByListenerThread", assignments);
        return result;
    }

    private static String format(Collection<TopicPartition> partitions) {
        return partitions.stream()
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .map(TopicPartition::toString)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private static String memberId(Consumer<?, ?> consumer) {
        try {
            return consumer.groupMetadata().memberId();
        } catch (Exception e) {
            return "n/a";
        }
    }
}
