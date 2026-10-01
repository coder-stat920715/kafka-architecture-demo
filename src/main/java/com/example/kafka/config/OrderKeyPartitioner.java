package com.example.kafka.config;

import com.example.kafka.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Custom partitioning strategy:
 * <ul>
 *   <li>VIP customers (configured IDs, or events whose tier is VIP) always go to a dedicated
 *       partition (default: 0), giving them an isolated, uncontended lane.</li>
 *   <li>Standard traffic is spread over the REMAINING partitions using MurmurHash2 of the key
 *       (the same hash Kafka's default partitioner uses), so per-key ordering is preserved.</li>
 *   <li>Null keys are distributed round-robin across the standard partitions.</li>
 * </ul>
 */
@Slf4j
public class OrderKeyPartitioner implements Partitioner {

    public static final String VIP_CUSTOMERS_CONFIG = "order.partitioner.vip.customers";
    public static final String VIP_PARTITION_CONFIG = "order.partitioner.vip.partition";

    private volatile Set<String> vipCustomers = Set.of();
    private volatile int vipPartition = 0;
    private final AtomicInteger roundRobin = new AtomicInteger(ThreadLocalRandom.current().nextInt());

    @Override
    public void configure(Map<String, ?> configs) {
        Object vip = configs.get(VIP_CUSTOMERS_CONFIG);
        if (vip != null && !String.valueOf(vip).isBlank()) {
            vipCustomers = Arrays.stream(String.valueOf(vip).split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
        }
        Object partition = configs.get(VIP_PARTITION_CONFIG);
        if (partition != null) {
            vipPartition = Integer.parseInt(String.valueOf(partition).trim());
        }
        log.info("OrderKeyPartitioner configured: vipCustomers={}, vipPartition={}", vipCustomers, vipPartition);
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        List<?> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (numPartitions <= 1) {
            return 0;
        }

        int vip = Math.min(vipPartition, numPartitions - 1);
        if (isVip(key, value)) {
            log.debug("VIP record routed to dedicated partition {} of topic {}", vip, topic);
            return vip;
        }

        // Standard pool = all partitions except the VIP partition.
        int poolSize = numPartitions - 1;
        int slot = (keyBytes == null)
                ? Utils.toPositive(roundRobin.getAndIncrement()) % poolSize
                : Utils.toPositive(Utils.murmur2(keyBytes)) % poolSize;

        // Map slot [0, poolSize) onto partitions skipping the VIP partition.
        int chosen = slot < vip ? slot : slot + 1;
        log.debug("Standard record routed to partition {} of topic {}", chosen, topic);
        return chosen;
    }

    private boolean isVip(Object key, Object value) {
        if (key != null && vipCustomers.contains(key.toString())) {
            return true;
        }
        return value instanceof OrderEvent event
                && event.getCustomerTier() == OrderEvent.CustomerTier.VIP;
    }

    @Override
    public void close() {
        // no resources to release
    }
}
