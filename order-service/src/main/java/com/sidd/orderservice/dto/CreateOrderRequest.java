package com.sidd.orderservice.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CreateOrderRequest(
        @NotBlank String customerName,
        @NotBlank String productId,
        @Min(1) int quantity
) {
}