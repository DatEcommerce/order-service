package com.dat.backend.orderservice.repository;

import com.dat.backend.orderservice.entity.OutboxOrder;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OutboxOrderRepository extends JpaRepository<OutboxOrder, String> {
}
