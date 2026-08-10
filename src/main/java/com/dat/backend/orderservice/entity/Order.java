package com.dat.backend.orderservice.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UuidGenerator;

import java.util.List;

@Entity(name = "orders")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {
    @Id
    @UuidGenerator
    private String id;

    // Status : PENDING ,COMPLETED, CANCELLED
    @Enumerated(EnumType.STRING)
    private OrderStatus orderStatus;
    private String userId;

    private String productId;
    private String productName;
    private Double productPrice;
    private Integer productQuantity;
    private Double totalPrice;

    private String paymentId;
    private String paymentUrl;
}
