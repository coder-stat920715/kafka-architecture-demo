package com.example.kafka.consumer;

import com.example.kafka.model.OrderEvent;
import com.example.kafka.simulator.KafkaStorageEngineSimulator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumes orders with manual offset acknowledgment and monitors the dead-letter topic.
 *
 * <p>Failure simulation via {@code product}: FAIL_ONCE (transient), FAIL_ALWAYS (retries then DLQ),
 * POISON_PILL or amount &lt;= 0 (non-retryable, straight to DLQ).
 */
@Slf4j
@Service
public class OrderConsumerService {

    /** Retried with exponential backoff, then dead-lettered. */
    public static class OrderProcessingException extends RuntimeException {
        public OrderProcessingException(String message) {
            super(message);
        }
    }

    /** Skips retries: dead-lettered on first failure. */
    public static class NonRetryableOrderException extends RuntimeException {
        public NonRetryableOrderException(String message) {
            super(message);
        }
    }

    private final KafkaStorageEngineSimulator storageEngine;
    private final ObjectMapper objectMapper;

    @Value("${app.log-engine.mirror-consumed-orders:true}")
    private boolean mirrorConsumedOrders;

    private final Set<String> transientFailureSeen = ConcurrentHashMap.newKeySet();
    private final AtomicLong processedOrders = new AtomicLong();
    private final AtomicLong deadLetteredOrders = new AtomicLong();

    public OrderConsumerService(KafkaStorageEngineSimulator storageEngine, ObjectMapper objectMapper) {
        this.storageEngine = storageEngine;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = "order-processor",
            topics = "${app.kafka.topics.orders}",
            groupId = "${app.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onOrder(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        OrderEvent order = record.value();
        log.info("[CONSUME] {}-{}@{} key={} orderId={} product={} (thread={})",
                record.topic(), record.partition(), record.offset(), record.key(),
                order == null ? null : order.getOrderId(),
                order == null ? null : order.getProduct(),
                Thread.currentThread().getName());

        process(order);

        if (mirrorConsumedOrders) {
            mirrorToStorageEngine(record.key(), order);
        }

        // Offset is committed only after successful processing (at-least-once).
        ack.acknowledge();
        processedOrders.incrementAndGet();
    }

    private void process(OrderEvent order) {
        if (order == null) {
            throw new NonRetryableOrderException("Order payload is null");
        }
        if ("POISON_PILL".equalsIgnoreCase(order.getProduct())) {
            throw new NonRetryableOrderException("Poison pill order " + order.getOrderId());
        }
        if (order.getAmount() == null || order.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new NonRetryableOrderException("Invalid amount for order " + order.getOrderId());
        }
        if ("FAIL_ALWAYS".equalsIgnoreCase(order.getProduct())) {
            throw new OrderProcessingException("Downstream system permanently unavailable for order "
                    + order.getOrderId());
        }
        if ("FAIL_ONCE".equalsIgnoreCase(order.getProduct())) {
            // add() returns true only the first time => first delivery fails, the retry succeeds.
            if (transientFailureSeen.add(order.getOrderId())) {
                throw new OrderProcessingException("Transient failure for order " + order.getOrderId());
            }
            transientFailureSeen.remove(order.getOrderId());
        }
        order.setStatus(OrderEvent.OrderStatus.COMPLETED);
        log.debug("Order {} processed", order.getOrderId());
    }

    private void mirrorToStorageEngine(String key, OrderEvent order) {
        try {
            storageEngine.append(key, objectMapper.writeValueAsString(order), true);
        } catch (JsonProcessingException e) {
            log.warn("Could not mirror order {} into log simulator: {}", order.getOrderId(), e.getMessage());
        }
    }

    /** Monitors dead-lettered records and prints the failure metadata added by the recoverer. */
    @KafkaListener(
            id = "dlq-monitor",
            topics = "${app.kafka.topics.dlq}",
            groupId = "${app.kafka.consumer.dlq-group-id}",
            containerFactory = "dlqListenerContainerFactory")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        deadLetteredOrders.incrementAndGet();
        log.error("[DLQ-MONITOR] {}-{}@{} key={} originalTopic={} originalPartition={} originalOffset={}"
                        + " exception={} message={} payload={}",
                record.topic(), record.partition(), record.offset(), record.key(),
                stringHeader(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                intHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                longHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                stringHeader(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                stringHeader(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                record.value());
    }

    public Map<String, Object> stats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("processedOrders", processedOrders.get());
        stats.put("deadLetteredOrders", deadLetteredOrders.get());
        return stats;
    }

    private static String stringHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null
                ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static Integer intHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null || header.value().length != Integer.BYTES
                ? null : ByteBuffer.wrap(header.value()).getInt();
    }

    private static Long longHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null || header.value().length != Long.BYTES
                ? null : ByteBuffer.wrap(header.value()).getLong();
    }
}
