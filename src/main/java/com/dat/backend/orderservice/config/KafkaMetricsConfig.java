package com.dat.backend.orderservice.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class KafkaMetricsConfig {

    private final MeterRegistry registry;
    private final Counter txCounter;
    private final Counter kafkaCounter;

    public KafkaMetricsConfig(MeterRegistry registry) {
        this.registry = registry;
        this.txCounter = Counter.builder("user-transactions.count")
                .description("Number of Kafka producer transactions")
                .register(registry);

        this.kafkaCounter = Counter.builder("kafka-messages.count")
                .description("Number of Kafka producer messages sent")
                .register(registry);
    }

    public void incrementTxCounter() {
        txCounter.increment();
    }

    public void incrementKafkaCounter() {
        kafkaCounter.increment();
    }

    public void incrementTxErrorCounter(String topicName) {
        Counter.builder("kafka-transactions.errors.count")
                .description("Number of Kafka transactions errors")
                .tag("topic", topicName)
                .register(registry)
                .increment();
    }

    public void incrementDltErrorCounter(String topicName) {
        Counter.builder("dlt-errors-count")
                .description("Number of Kafka transactions errors dlt")
                .tag("topic", topicName)
                .register(registry)
                .increment();
    }

    public void incrementKickOutConsumer(String containerName) {
        Counter.builder("kick-out-consumers.count")
                .description("Number of Kafka consumer kicked out")
                .tag("container-name", containerName)
                .register(registry)
                .increment();
    }
}
