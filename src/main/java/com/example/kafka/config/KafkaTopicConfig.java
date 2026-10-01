package com.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares topics; {@code KafkaAdmin} creates them on startup if they do not exist.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;

    @Value("${app.kafka.topics.audit}")
    private String auditTopic;

    @Value("${app.kafka.topics.dlq}")
    private String dlqTopic;

    @Value("${app.kafka.topics.orders-partitions}")
    private int ordersPartitions;

    @Value("${app.kafka.topics.audit-partitions}")
    private int auditPartitions;

    @Value("${app.kafka.topics.dlq-partitions}")
    private int dlqPartitions;

    @Value("${app.kafka.topics.replication-factor}")
    private int replicationFactor;

    /** Partition 0 is reserved for VIP traffic by {@link OrderKeyPartitioner}. */
    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ordersTopic)
                .partitions(ordersPartitions)
                .replicas(replicationFactor)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(7L * 24 * 60 * 60 * 1000))
                .config(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(64 * 1024 * 1024))
                .build();
    }

    /** Second topic written atomically together with {@code orders} by the transactional producer. */
    @Bean
    public NewTopic ordersAuditTopic() {
        return TopicBuilder.name(auditTopic)
                .partitions(auditPartitions)
                .replicas(replicationFactor)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    /** Dead Letter Topic for records that exhausted retries or are non-retryable. */
    @Bean
    public NewTopic ordersDlqTopic() {
        return TopicBuilder.name(dlqTopic)
                .partitions(dlqPartitions)
                .replicas(replicationFactor)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(30L * 24 * 60 * 60 * 1000))
                .build();
    }
}
