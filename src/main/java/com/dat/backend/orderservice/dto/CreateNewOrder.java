package com.dat.backend.orderservice.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CreateNewOrder {
    private String paymentMethod;
    private String productId;
    private String productName;
    private Double productPrice;
    private Integer productQuantity;
    private String bankingMethod;
}
