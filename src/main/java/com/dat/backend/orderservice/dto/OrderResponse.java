package com.dat.backend.orderservice.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderResponse {
    private String id;
    private String orderStatus;
    private String userId;
    private String productId;
    private String productName;
    private Double productPrice;
    private Integer productQuantity;
    private Double totalPrice;
    private String paymentUrl;
    private String paymentId;
}
