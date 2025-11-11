package com.dat.backend.orderservice.mapper;

import com.dat.backend.orderservice.dto.OrderResponse;
import com.dat.backend.orderservice.entity.Order;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface OrderMapper {
    OrderResponse orderToOrderResponse(Order order);
}
