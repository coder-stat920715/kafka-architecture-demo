package com.example.kafka.config;

import com.example.kafka.consumer.CustomRebalanceListener;
import com.example.kafka.consumer.OrderConsumerService;
import com.example.kafka.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Consumer infrastructure: manual acknowledgment, exponential-backoff retries,
 * dead-letter routing and rebalance observation.
 */
@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.consumer.dlq-group-id}")
    private String dlqGroupId;

    @Value("${app.kafka.consumer.concurrency}")
    private int concurrency;

    @Value("${app.kafka.topics.dlq}")
    private String dlqTopic;

    @Value("${app.kafka.retry.max-retries}")
    private int maxRetries;

    @Value("${app.kafka.retry.initial-interval-ms}")
    private long initialIntervalMs;

    @Value("${app.kafka.retry.multiplier}")
    private double multiplier;

    @Value("${app.kafka.retry.max-interval-ms}")
    private long maxIntervalMs;

    // ------------------------------------------------------------------
    // Main order consumer
    // ------------------------------------------------------------------

    @Bean("orderConsumerFactory")
    public ConsumerFactory<String, OrderEvent> orderConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // ErrorHandlingDeserializer turns poison messages into a DeserializationException header
        // instead of crashing the poll loop; the error handler then routes them to the DLQ.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.kafka.model");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Only see committed transactional writes (aborted transactions stay invisible).
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
        // Incremental cooperative rebalancing: only moved partitions are revoked.
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public DefaultErrorHandler orderErrorHandler(
            @Qualifier("dlqJsonKafkaTemplate") KafkaTemplate<String, Object> dlqJsonTemplate,
            @Qualifier("dlqBytesKafkaTemplate") KafkaTemplate<String, byte[]> dlqBytesTemplate) {

        // Template is chosen by the value type of the failed record:
        // byte[] (undeserializable payload) -> bytes template; anything else -> JSON template.
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, dlqBytesTemplate);
        templates.put(Object.class, dlqJsonTemplate);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(templates,
                (failed, ex) -> {
                    log.error("[DLQ] Routing record {}-{}@{} to '{}' after failure: {}",
                            failed.topic(), failed.partition(), failed.offset(), dlqTopic,
                            ex.getMessage());
                    // Negative partition lets the DLQ producer choose the partition.
                    return new TopicPartition(dlqTopic, -1);
                });

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // With MANUAL ack modes, commit the offset of a record once it has been dead-lettered.
        handler.setCommitRecovered(true);
        handler.addNotRetryableExceptions(OrderConsumerService.NonRetryableOrderException.class);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> rec, Exception ex, int deliveryAttempt) {
                log.warn("[RETRY] attempt {} failed for {}-{}@{}: {}", deliveryAttempt,
                        rec.topic(), rec.partition(), rec.offset(), ex.getMessage());
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> rec, Exception ex) {
                log.error("[RECOVERED] {}-{}@{} dead-lettered: {}", rec.topic(), rec.partition(),
                        rec.offset(), ex.getMessage());
            }
        });
        return handler;
    }

    @Bean("kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
            @Qualifier("orderConsumerFactory") ConsumerFactory<String, OrderEvent> consumerFactory,
            DefaultErrorHandler orderErrorHandler,
            CustomRebalanceListener rebalanceListener) {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.setCommonErrorHandler(orderErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        return factory;
    }

    // ------------------------------------------------------------------
    // DLQ monitor consumer (raw strings so any dead-lettered payload can be read)
    // ------------------------------------------------------------------

    @Bean("dlqConsumerFactory")
    public ConsumerFactory<String, String> dlqConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, dlqGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean("dlqListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> dlqListenerContainerFactory(
            @Qualifier("dlqConsumerFactory") ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        return factory;
    }
}
