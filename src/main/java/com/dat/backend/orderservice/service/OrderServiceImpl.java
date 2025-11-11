package com.dat.backend.orderservice.service;

//import io.micrometer.tracing.BaggageInScope;
//import io.micrometer.tracing.Span;
//import io.micrometer.tracing.Tracer;
import com.dat.backend.orderservice.dto.CreateNewOrder;
import com.dat.backend.orderservice.dto.CreatePayment;
import com.dat.backend.orderservice.dto.OrderResponse;
import com.dat.backend.orderservice.entity.Order;
import com.dat.backend.orderservice.entity.OrderStatus;
import com.dat.backend.orderservice.entity.OutBoxEvent;
import com.dat.backend.orderservice.mapper.OrderMapper;
import com.dat.backend.orderservice.repository.OrderRepository;
import com.dat.backend.orderservice.repository.OutBoxEventRepository;
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
    private final OutBoxEventRepository outBoxEventRepository;

    public OrderServiceImpl(@Qualifier("txKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
                            OrderMapper orderMapper,
                            OrderRepository orderRepository,
                            PaymentService paymentService,
                            OutBoxEventRepository outBoxEventRepository) {
        //this.userService = userService;
        this.kafkaTemplate = kafkaTemplate;
        this.orderMapper = orderMapper;
        this.orderRepository = orderRepository;
        this.paymentService = paymentService;
        this.outBoxEventRepository = outBoxEventRepository;
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
            order.setOrderStatus(OrderStatus.WAITING_BANKING);
            // Send event to payment service
//            com.dat.shared.order.OrderResponse orderResponse = new com.dat.shared.order.OrderResponse();
//            orderResponse.setOrderId(order.getId());
//            orderResponse.setUserId(id);
//            orderResponse.setTotalPrice(order.getTotalPrice());
//
//            String key = "order-" + order.getId();
//
//            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("payment-topic", key, orderResponse);
//            kafkaTemplate.send(producerRecord);
            CreatePayment createPayment = new CreatePayment();
            createPayment.setOrderId(order.getId());
            createPayment.setTotalPrice(order.getTotalPrice());
            String paymentUrl = paymentService.createPayment(createPayment);
            log.info("Payment URL received: {}", paymentUrl);
            order.setPaymentUrl(paymentUrl);
            orderRepository.save(order);

            // Send data to outbox
            OutBoxEvent event = new OutBoxEvent();
            event.setOrderId(order.getId());
            event.setProductId(newOrder.getProductId());
            event.setQuantity(newOrder.getProductQuantity());
            event.setPaymentStatus(paymentMethod);
            outBoxEventRepository.save(event);
        }
        return orderMapper.orderToOrderResponse(order);
    }
}
