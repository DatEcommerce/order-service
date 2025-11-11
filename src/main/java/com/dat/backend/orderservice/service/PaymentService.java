package com.dat.backend.orderservice.service;

import com.dat.backend.orderservice.dto.CreatePayment;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

@FeignClient(name = "payment-service", url = "http://localhost:8084")
public interface PaymentService {
    @GetMapping("/api/v1/payments/create")
    public String createPayment(CreatePayment createPayment);
}
