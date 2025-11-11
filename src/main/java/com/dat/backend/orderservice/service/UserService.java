package com.dat.backend.orderservice.service;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

@FeignClient(name = "user-service", url = "http://localhost:8082")
public interface UserService {
    @GetMapping("/api/v1/users/test")
    public String test();
}
