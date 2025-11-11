package com.dat.backend.orderservice.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UuidGenerator;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity(name = "outbox")
public class OutBoxEvent {
    @Id
    @UuidGenerator
    private String id;

    private String orderId;
    private String paymentId;
    private String productId;
    private String paymentStatus;
    private Integer quantity;
}
