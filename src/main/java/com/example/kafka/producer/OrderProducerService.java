package com.example.kafka.producer;

import com.example.kafka.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes order events.
 * <ul>
 *   <li>{@link #publishOrder} - idempotent send, partition chosen by {@code OrderKeyPartitioner}.</li>
 *   <li>{@link #publishOrdersAtomically} - multi-topic, multi-partition write committed or
 *       aborted as ONE Kafka transaction (exactly-once on the write path).</li>
 * </ul>
 */
@Slf4j
@Service
public class OrderProducerService {

    private final KafkaTemplate<String, Object> orderKafkaTemplate;
    private final KafkaTemplate<String, Object> transactionalKafkaTemplate;

    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;

    @Value("${app.kafka.topics.audit}")
    private String auditTopic;

    @Value("${app.kafka.producer.send-timeout-seconds}")
    private long sendTimeoutSeconds;

    public OrderProducerService(
            @Qualifier("orderKafkaTemplate") KafkaTemplate<String, Object> orderKafkaTemplate,
            @Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, Object> transactionalKafkaTemplate) {
        this.orderKafkaTemplate = orderKafkaTemplate;
        this.transactionalKafkaTemplate = transactionalKafkaTemplate;
    }

    /** Broker acknowledgment details for one record. */
    public record PublishReceipt(String topic, int partition, long offset, String key, long timestamp) {
    }

    public static class OrderPublishException extends RuntimeException {
        public OrderPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Thrown on purpose to demonstrate transaction abort. */
    public static class TransactionAbortedDemoException extends RuntimeException {
        public TransactionAbortedDemoException(String message) {
            super(message);
        }
    }

    /**
     * Sends one event keyed by customerId so all events of a customer stay ordered in one partition.
     */
    public PublishReceipt publishOrder(OrderEvent event) {
        prepare(event);
        CompletableFuture<SendResult<String, Object>> future =
                orderKafkaTemplate.send(ordersTopic, event.getCustomerId(), event);
        PublishReceipt receipt = await(future);
        log.info("Published order {} -> {}-{}@{}", event.getOrderId(), receipt.topic(),
                receipt.partition(), receipt.offset());
        return receipt;
    }

    /**
     * Writes every order to {@code orders} AND an audit record to {@code orders-audit} inside a single
     * transaction. If anything fails (or {@code simulateFailure} is true), nothing becomes visible to
     * {@code read_committed} consumers.
     */
    @Transactional(transactionManager = "kafkaTransactionManager")
    public List<PublishReceipt> publishOrdersAtomically(List<OrderEvent> events, boolean simulateFailure) {
        List<CompletableFuture<SendResult<String, Object>>> futures = new ArrayList<>();

        for (OrderEvent event : events) {
            prepare(event);
            futures.add(transactionalKafkaTemplate.send(ordersTopic, event.getCustomerId(), event));
            futures.add(transactionalKafkaTemplate.send(auditTopic, event.getOrderId(), auditPayload(event)));
        }

        List<PublishReceipt> receipts = new ArrayList<>();
        for (CompletableFuture<SendResult<String, Object>> future : futures) {
            receipts.add(await(future));
        }

        if (simulateFailure) {
            log.warn("Simulating failure after {} records reached the broker - transaction will ABORT",
                    receipts.size());
            throw new TransactionAbortedDemoException(
                    "Simulated failure after " + receipts.size()
                            + " records were sent; the transaction was aborted and none are visible"
                            + " to read_committed consumers");
        }

        log.info("Transaction of {} records will COMMIT", receipts.size());
        return receipts;
    }

    private Map<String, Object> auditPayload(OrderEvent event) {
        Map<String, Object> audit = new LinkedHashMap<>();
        audit.put("orderId", event.getOrderId());
        audit.put("customerId", event.getCustomerId());
        audit.put("customerTier", event.getCustomerTier());
        audit.put("amount", event.getAmount());
        audit.put("action", "ORDER_ACCEPTED");
        audit.put("auditedAt", Instant.now().toString());
        return audit;
    }

    private void prepare(OrderEvent event) {
        if (event.getOrderId() == null || event.getOrderId().isBlank()) {
            event.setOrderId(UUID.randomUUID().toString());
        }
        if (event.getCreatedAt() == null) {
            event.setCreatedAt(Instant.now());
        }
        if (event.getStatus() == null) {
            event.setStatus(OrderEvent.OrderStatus.CREATED);
        }
        if (event.getCustomerTier() == null) {
            event.setCustomerTier(OrderEvent.CustomerTier.STANDARD);
        }
    }

    private PublishReceipt await(CompletableFuture<SendResult<String, Object>> future) {
        try {
            SendResult<String, Object> result = future.get(sendTimeoutSeconds, TimeUnit.SECONDS);
            return new PublishReceipt(
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    result.getProducerRecord().key(),
                    result.getRecordMetadata().timestamp());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OrderPublishException("Interrupted while waiting for broker acknowledgment", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new OrderPublishException("Broker did not acknowledge the record: " + e.getMessage(), e);
        }
    }
}
