package com.dat.backend.orderservice.controller;

import com.dat.backend.orderservice.dto.ApiResponse;
import com.dat.backend.orderservice.dto.CreateNewOrder;
import com.dat.backend.orderservice.dto.OrderResponse;
import com.dat.backend.orderservice.service.OrderServiceImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/orders")
public class OrderController {
    private final OrderServiceImpl orderService;

    @GetMapping("/test-transaction")
    public String test() {
        return orderService.test();
    }

    @PostMapping("/create")
    public ApiResponse<OrderResponse> create(@RequestBody CreateNewOrder newOrder,
                                             @RequestParam String userId) {
        return ApiResponse.success(orderService.createNewOrder(newOrder,userId));
    }
}
