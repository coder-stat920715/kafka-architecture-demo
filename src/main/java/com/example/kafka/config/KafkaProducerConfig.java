package com.example.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer infrastructure.
 *
 * <ul>
 *   <li>{@code orderKafkaTemplate}: idempotent, non-transactional (simple publish endpoint).</li>
 *   <li>{@code transactionalKafkaTemplate}: idempotent + transactional (atomic multi-topic writes).</li>
 *   <li>{@code dlqJsonKafkaTemplate} / {@code dlqBytesKafkaTemplate}: used by the consumer's
 *       dead-letter recoverer. They are non-transactional because the listener thread is not
 *       inside a Kafka transaction.</li>
 * </ul>
 * All order producers use acks=all, idempotence, infinite retries (bounded by delivery.timeout.ms)
 * and max.in.flight=5 (the maximum that still preserves ordering with idempotence).
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.producer.transaction-id-prefix}")
    private String transactionIdPrefix;

    @Value("${app.kafka.partitioner.vip-customers}")
    private String vipCustomers;

    @Value("${app.kafka.partitioner.vip-partition}")
    private int vipPartition;

    /** Reliability settings shared by every producer in this application. */
    private Map<String, Object> reliableProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }

    /** Reliability settings + JSON value serialization + custom VIP partitioner. */
    private Map<String, Object> orderProducerProps() {
        Map<String, Object> props = reliableProps();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OrderKeyPartitioner.class);
        props.put(OrderKeyPartitioner.VIP_CUSTOMERS_CONFIG, vipCustomers);
        props.put(OrderKeyPartitioner.VIP_PARTITION_CONFIG, vipPartition);
        return props;
    }

    // ------------------------------------------------------------------
    // Idempotent (non-transactional) producer
    // ------------------------------------------------------------------

    @Bean("orderProducerFactory")
    public ProducerFactory<String, Object> orderProducerFactory() {
        return new DefaultKafkaProducerFactory<>(orderProducerProps());
    }

    @Bean("orderKafkaTemplate")
    public KafkaTemplate<String, Object> orderKafkaTemplate() {
        return new KafkaTemplate<>(orderProducerFactory());
    }

    // ------------------------------------------------------------------
    // Transactional producer (Exactly-Once Semantics on the write side)
    // ------------------------------------------------------------------

    @Bean("transactionalProducerFactory")
    public ProducerFactory<String, Object> transactionalProducerFactory() {
        DefaultKafkaProducerFactory<String, Object> factory =
                new DefaultKafkaProducerFactory<>(orderProducerProps());
        // Setting a prefix makes the factory transactional (transactional.id = prefix + suffix).
        factory.setTransactionIdPrefix(transactionIdPrefix);
        return factory;
    }

    @Bean("transactionalKafkaTemplate")
    public KafkaTemplate<String, Object> transactionalKafkaTemplate() {
        return new KafkaTemplate<>(transactionalProducerFactory());
    }

    /**
     * Drives {@code @Transactional("kafkaTransactionManager")} so that every send in a service
     * method is committed or aborted atomically across topics and partitions.
     */
    @Bean("kafkaTransactionManager")
    public KafkaTransactionManager<String, Object> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(transactionalProducerFactory());
    }

    // ------------------------------------------------------------------
    // Dead-letter producers (no custom partitioner: spread DLQ records evenly)
    // ------------------------------------------------------------------

    @Bean("dlqJsonProducerFactory")
    public ProducerFactory<String, Object> dlqJsonProducerFactory() {
        Map<String, Object> props = reliableProps();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean("dlqJsonKafkaTemplate")
    public KafkaTemplate<String, Object> dlqJsonKafkaTemplate() {
        return new KafkaTemplate<>(dlqJsonProducerFactory());
    }

    /** Used when the original value could not be deserialized and is forwarded as raw bytes. */
    @Bean("dlqBytesProducerFactory")
    public ProducerFactory<String, byte[]> dlqBytesProducerFactory() {
        Map<String, Object> props = reliableProps();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean("dlqBytesKafkaTemplate")
    public KafkaTemplate<String, byte[]> dlqBytesKafkaTemplate() {
        return new KafkaTemplate<>(dlqBytesProducerFactory());
    }
}
