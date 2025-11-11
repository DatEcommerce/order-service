package com.dat.backend.orderservice.repository;

import com.dat.backend.orderservice.entity.OutBoxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OutBoxEventRepository extends JpaRepository<OutBoxEvent, String> {
}
