package com.dat.backend.orderservice.service;

//import io.micrometer.tracing.BaggageInScope;
//import io.micrometer.tracing.Span;
//import io.micrometer.tracing.Tracer;
import com.dat.backend.orderservice.dto.CreateNewOrder;
import com.dat.backend.orderservice.dto.CreatePayment;
import com.dat.backend.orderservice.dto.OrderResponse;
import com.dat.backend.orderservice.dto.PaymentResponse;
import com.dat.backend.orderservice.entity.Order;
import com.dat.backend.orderservice.entity.OrderStatus;
import com.dat.backend.orderservice.entity.OutboxOrder;
import com.dat.backend.orderservice.mapper.OrderMapper;
import com.dat.backend.orderservice.repository.OrderRepository;
import com.dat.backend.orderservice.repository.OutboxOrderRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class OrderServiceImpl {
//    private final UserService userService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderMapper orderMapper;
    private final OrderRepository orderRepository;
    private final PaymentService paymentService;
    private final OutboxOrderRepository outboxOrderRepository;

    public OrderServiceImpl(@Qualifier("txKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
                            OrderMapper orderMapper,
                            OrderRepository orderRepository,
                            PaymentService paymentService,
                            OutboxOrderRepository outboxOrderRepository) {
        //this.userService = userService;
        this.kafkaTemplate = kafkaTemplate;
        this.orderMapper = orderMapper;
        this.orderRepository = orderRepository;
        this.paymentService = paymentService;
        this.outboxOrderRepository = outboxOrderRepository;
    }

    @Transactional("kafkaTransactionManager")
    public String test() {
        ProducerRecord<String, Object> record = new ProducerRecord<>("test-order-topic", "test");
        kafkaTemplate.send(record);
        return "Done";
    }

    @KafkaListener(id = "order-service-listener",
            topics = "test-order-topic",
            groupId = "order-group",
            containerFactory = "transactionalKafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, Object> record) {
        throw new RuntimeException("Test exception");
    }

    @KafkaListener(id = "order-service-dlt-listener",
            topics = "test-order-topic-dlt",
            groupId = "order-group-dlt",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenDLT(ConsumerRecord<String, Object> record) {
        System.out.println("DLT: " + record);
    }

    public OrderResponse createNewOrder(CreateNewOrder newOrder, String id) {
        // Map CreateNewOrder to Order
        Order order = new Order();

        String paymentMethod = newOrder.getPaymentMethod();
        order.setUserId(id);
        order.setProductId(newOrder.getProductId());
        order.setProductName(newOrder.getProductName());
        order.setProductPrice(newOrder.getProductPrice());
        order.setProductQuantity(newOrder.getProductQuantity());
        order.setTotalPrice(newOrder.getProductPrice() * newOrder.getProductQuantity());
        if ("COD".equalsIgnoreCase(paymentMethod)) {
            order.setOrderStatus(OrderStatus.IN_PROGRESS);
            orderRepository.save(order);
        } else {
            String bankingMethod = newOrder.getBankingMethod();
            order.setOrderStatus(OrderStatus.WAITING_BANKING);
            // Call payment-service for creating payment entity
            CreatePayment createPayment = new CreatePayment();
            createPayment.setOrderId(order.getId());
            createPayment.setTotalPrice(order.getTotalPrice());
            PaymentResponse payment = paymentService.createPayment(createPayment);
            order.setPaymentUrl(payment.getPaymentUrl());
            order.setPaymentId(payment.getPaymentId());
            orderRepository.save(order);

            // Send data to outbox
            OutboxOrder event = new OutboxOrder();
            event.setOrderId(order.getId());
            event.setOrderStatus(order.getOrderStatus().toString());
            event.setProductId(order.getProductId());
            event.setProductQuantity(order.getProductQuantity());

            outboxOrderRepository.save(event);
        }
        return orderMapper.orderToOrderResponse(order);
    }

    @KafkaListener(id = "Listener-payment-status-update",
            topics = "outbox.db_payment.outbox_payment")
    public void updatePaymentStatus(ConsumerRecord<String, Object> paymentEvent) {
        try {
            String response = paymentEvent.value().toString();
            System.out.println(response);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            String orderId = node.path("after").path("orderId").asText();
            String paymentId = node.path("after").path("paymentId").asText();
            String paymentStatus = node.path("after").path("paymentStatus").asText();
            Order order = orderRepository.findById(orderId).orElseThrow(() -> new RuntimeException("Order not found"));
            if ("FAILED".equalsIgnoreCase(paymentStatus)) {
                order.setOrderStatus(OrderStatus.CANCELLED);
            }
            if ("SUCCESS".equalsIgnoreCase(paymentStatus)) {
                order.setOrderStatus(OrderStatus.COMPLETED);
            }
            orderRepository.save(order);

            // update outbox order
            OutboxOrder outboxOrder = outboxOrderRepository.findById(orderId).orElseThrow(() -> new RuntimeException("Order not found"));
            outboxOrder.setOrderStatus(order.getOrderStatus().toString());
            outboxOrderRepository.save(outboxOrder);
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
