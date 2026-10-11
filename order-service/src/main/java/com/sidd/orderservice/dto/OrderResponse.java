package com.sidd.orderservice.dto;

import com.sidd.orderservice.model.Order;
import com.sidd.orderservice.model.OrderStatus;

import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        String customerName,
        String productId,
        int quantity,
        OrderStatus status,
        Instant createdAt
) {
    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerName(),
                order.getProductId(),
                order.getQuantity(),
                order.getStatus(),
                order.getCreatedAt());
    }
}