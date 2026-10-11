package com.sidd.orderservice.service;

import com.sidd.orderservice.dto.CreateOrderRequest;
import com.sidd.orderservice.dto.OrderResponse;
import com.sidd.orderservice.model.Order;
import com.sidd.orderservice.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        Order order = new Order();
        order.setCustomerName(request.customerName());
        order.setProductId(request.productId());
        order.setQuantity(request.quantity());
        Order saved = orderRepository.save(order);
        return OrderResponse.from(saved);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(UUID id) {
        return orderRepository.findById(id)
                .map(OrderResponse::from)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Order not found: " + id));
    }
}