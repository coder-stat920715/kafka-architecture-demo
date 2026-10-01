package com.example.kafka.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Domain event published to the {@code orders} topic.
 *
 * <p>Special {@code product} values trigger consumer failure simulations:
 * <ul>
 *   <li>{@code FAIL_ONCE}   - fails on first delivery, succeeds on retry</li>
 *   <li>{@code FAIL_ALWAYS} - retried with exponential backoff, then routed to the DLQ</li>
 *   <li>{@code POISON_PILL} - non-retryable, routed to the DLQ immediately</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OrderEvent {

    private String orderId;

    @NotBlank
    private String customerId;

    private CustomerTier customerTier;

    @NotBlank
    private String product;

    @Min(1)
    private int quantity;

    @NotNull
    private BigDecimal amount;

    private OrderStatus status;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant createdAt;

    public enum CustomerTier {
        STANDARD, VIP
    }

    public enum OrderStatus {
        CREATED, PROCESSING, COMPLETED, FAILED
    }
}
