package com.example.kafka.controller;

import com.example.kafka.consumer.CustomRebalanceListener;
import com.example.kafka.consumer.OrderConsumerService;
import com.example.kafka.model.OrderEvent;
import com.example.kafka.producer.OrderProducerService;
import com.example.kafka.producer.OrderProducerService.OrderPublishException;
import com.example.kafka.producer.OrderProducerService.PublishReceipt;
import com.example.kafka.producer.OrderProducerService.TransactionAbortedDemoException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class OrderController {

    private static final long ADMIN_TIMEOUT_SECONDS = 10;

    private final OrderProducerService producerService;
    private final OrderConsumerService consumerService;
    private final CustomRebalanceListener rebalanceListener;
    private final KafkaAdmin kafkaAdmin;

    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;

    @Value("${app.kafka.topics.audit}")
    private String auditTopic;

    @Value("${app.kafka.topics.dlq}")
    private String dlqTopic;

    @Value("${app.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${app.kafka.consumer.dlq-group-id}")
    private String dlqGroupId;

    @Value("${app.kafka.partitioner.vip-partition}")
    private int vipPartition;

    public record TransactionalBatchRequest(@NotEmpty @Valid List<OrderEvent> orders, boolean simulateFailure) {
    }

    // ------------------------------------------------------------------
    // POST /api/v1/orders/publish
    // ------------------------------------------------------------------

    @PostMapping("/orders/publish")
    public ResponseEntity<Map<String, Object>> publish(@Valid @RequestBody OrderEvent event) {
        PublishReceipt receipt = producerService.publishOrder(event);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "PUBLISHED");
        body.put("orderId", event.getOrderId());
        body.put("customerId", event.getCustomerId());
        body.put("customerTier", event.getCustomerTier());
        body.put("routedToDedicatedVipPartition", receipt.partition() == vipPartition);
        body.put("receipt", receipt);
        return ResponseEntity.ok(body);
    }

    // ------------------------------------------------------------------
    // POST /api/v1/orders/transactional
    // ------------------------------------------------------------------

    @PostMapping("/orders/transactional")
    public ResponseEntity<Map<String, Object>> transactional(
            @Valid @RequestBody TransactionalBatchRequest request) {
        Map<String, Object> body = new LinkedHashMap<>();
        try {
            List<PublishReceipt> receipts =
                    producerService.publishOrdersAtomically(request.orders(), request.simulateFailure());
            body.put("status", "COMMITTED");
            body.put("recordCount", receipts.size());
            body.put("topics", List.of(ordersTopic, auditTopic));
            body.put("receipts", receipts);
            return ResponseEntity.ok(body);
        } catch (TransactionAbortedDemoException e) {
            body.put("status", "ABORTED");
            body.put("reason", e.getMessage());
            return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
        }
    }

    // ------------------------------------------------------------------
    // GET /api/v1/kafka/cluster-status
    // ------------------------------------------------------------------

    @GetMapping("/kafka/cluster-status")
    public ResponseEntity<Map<String, Object>> clusterStatus() {
        Map<String, Object> response = new LinkedHashMap<>();
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {

            // ---- cluster ----
            DescribeClusterResult cluster = admin.describeCluster();
            Map<String, Object> clusterInfo = new LinkedHashMap<>();
            clusterInfo.put("clusterId", cluster.clusterId().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Node controller = cluster.controller().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            clusterInfo.put("controller", controller == null ? null : describeNode(controller));
            List<Map<String, Object>> brokers = new ArrayList<>();
            for (Node node : cluster.nodes().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                brokers.add(describeNode(node));
            }
            clusterInfo.put("brokers", brokers);
            response.put("cluster", clusterInfo);

            // ---- topics / partitions / offsets ----
            List<String> topicNames = List.of(ordersTopic, auditTopic, dlqTopic);
            Map<String, TopicDescription> descriptions = new TreeMap<>(
                    admin.describeTopics(topicNames).allTopicNames().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            Map<TopicPartition, OffsetSpec> earliestRequest = new HashMap<>();
            Map<TopicPartition, OffsetSpec> latestRequest = new HashMap<>();
            for (TopicDescription description : descriptions.values()) {
                for (TopicPartitionInfo info : description.partitions()) {
                    TopicPartition tp = new TopicPartition(description.name(), info.partition());
                    earliestRequest.put(tp, OffsetSpec.earliest());
                    latestRequest.put(tp, OffsetSpec.latest());
                }
            }
            Map<TopicPartition, ListOffsetsResultInfo> earliest =
                    admin.listOffsets(earliestRequest).all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Map<TopicPartition, ListOffsetsResultInfo> latest =
                    admin.listOffsets(latestRequest).all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Map<String, Object> topics = new LinkedHashMap<>();
            for (TopicDescription description : descriptions.values()) {
                List<Map<String, Object>> partitions = new ArrayList<>();
                for (TopicPartitionInfo info : description.partitions()) {
                    TopicPartition tp = new TopicPartition(description.name(), info.partition());
                    partitions.add(mapOf(
                            "partition", info.partition(),
                            "leader", info.leader() == null ? null : info.leader().id(),
                            "replicas", info.replicas().stream().map(Node::id).toList(),
                            "isr", info.isr().stream().map(Node::id).toList(),
                            "beginningOffset", earliest.get(tp).offset(),
                            "endOffset", latest.get(tp).offset()));
                }
                partitions.sort((a, b) -> Integer.compare((int) a.get("partition"), (int) b.get("partition")));
                topics.put(description.name(), mapOf(
                        "partitionCount", description.partitions().size(),
                        "internal", description.isInternal(),
                        "partitions", partitions));
            }
            response.put("topics", topics);

            // ---- consumer groups ----
            List<String> groupIds = List.of(consumerGroupId, dlqGroupId);
            Map<String, ConsumerGroupDescription> groupDescriptions =
                    admin.describeConsumerGroups(groupIds).all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Map<String, Object> groups = new LinkedHashMap<>();
            for (String groupId : groupIds) {
                ConsumerGroupDescription description = groupDescriptions.get(groupId);
                Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                List<Map<String, Object>> members = new ArrayList<>();
                description.members().forEach(m -> members.add(mapOf(
                        "memberId", m.consumerId(),
                        "clientId", m.clientId(),
                        "host", m.host(),
                        "assignedPartitions", m.assignment().topicPartitions().stream()
                                .map(TopicPartition::toString).sorted().toList())));

                List<Map<String, Object>> offsets = new ArrayList<>();
                long totalLag = 0;
                List<Map.Entry<TopicPartition, OffsetAndMetadata>> sortedCommitted =
                        new ArrayList<>(committed.entrySet());
                sortedCommitted.sort(Comparator
                        .comparing((Map.Entry<TopicPartition, OffsetAndMetadata> e) -> e.getKey().topic())
                        .thenComparingInt(e -> e.getKey().partition()));
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : sortedCommitted) {
                    TopicPartition tp = entry.getKey();
                    ListOffsetsResultInfo end = latest.get(tp);
                    Long lag = end == null ? null : Math.max(0, end.offset() - entry.getValue().offset());
                    if (lag != null) {
                        totalLag += lag;
                    }
                    offsets.add(mapOf(
                            "topic", tp.topic(),
                            "partition", tp.partition(),
                            "committedOffset", entry.getValue().offset(),
                            "endOffset", end == null ? null : end.offset(),
                            "lag", lag));
                }
                groups.put(groupId, mapOf(
                        "state", String.valueOf(description.state()),
                        "partitionAssignor", description.partitionAssignor(),
                        "memberCount", members.size(),
                        "members", members,
                        "totalLag", totalLag,
                        "committedOffsets", offsets));
            }
            response.put("consumerGroups", groups);

            // ---- application-side view ----
            response.put("application", mapOf(
                    "consumerStats", consumerService.stats(),
                    "rebalanceListener", rebalanceListener.snapshot()));
            response.put("note", "Offsets include transaction control markers, so they can advance faster"
                    + " than the number of visible records.");
            return ResponseEntity.ok(response);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return unavailable("Interrupted while querying the cluster");
        } catch (ExecutionException | TimeoutException e) {
            log.warn("Cluster status query failed: {}", e.getMessage());
            return unavailable("Kafka cluster query failed: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Error handling
    // ------------------------------------------------------------------

    @ExceptionHandler(OrderPublishException.class)
    public ResponseEntity<Map<String, Object>> publishFailed(OrderPublishException e) {
        log.error("Publish failed: {}", e.getMessage());
        return unavailable(e.getMessage());
    }

    private ResponseEntity<Map<String, Object>> unavailable(String message) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(mapOf("status", "UNAVAILABLE", "error", message));
    }

    private Map<String, Object> describeNode(Node node) {
        return mapOf("id", node.id(), "host", node.host(), "port", node.port(), "rack", node.rack());
    }

    /** Null-tolerant, insertion-ordered map builder from alternating key/value arguments. */
    private static Map<String, Object> mapOf(Object... keyValues) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }
}
