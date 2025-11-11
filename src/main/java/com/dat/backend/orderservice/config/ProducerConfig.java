package com.dat.backend.orderservice.config;

import com.dat.backend.orderservice.tracing.ProducerTemplateTracing;
import jakarta.persistence.EntityManagerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;
import java.util.UUID;

@Configuration
public class ProducerConfig {

    private final ProducerTemplateTracing producerTemplateTracing;

    public ProducerConfig(ProducerTemplateTracing producerTemplate) {
        this.producerTemplateTracing = producerTemplate;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerProperties());
    }

    @Bean
    public Map<String, Object> producerProperties() {
        Map<String, Object> props = new java.util.HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "1");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return props;
    }

    /**
     * Kafka Template with non-transactional producer factory
     */
    @Bean (name = "nonTxKafkaTemplate")
    public KafkaTemplate<String, Object> kafkaTemplate(@Qualifier("producerFactory") ProducerFactory<String, Object> pf) {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(pf);
        kafkaTemplate.setObservationEnabled(true);
        kafkaTemplate.setObservationConvention(producerTemplateTracing);
        return kafkaTemplate;
    }

    @Bean (name = "txProducerFactory")
    public ProducerFactory<String, Object> txProducerFactory() {
        return new DefaultKafkaProducerFactory<>(txProducerProperties());
    }

    @Bean
    public Map<String, Object> txProducerProperties() {
        Map<String, Object> props = producerProperties();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-service-tx-id-");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }


    /**
     * Kafka Template with transactional producer factory
     */
    @Bean
    public KafkaTemplate<String, Object> txKafkaTemplate(@Qualifier("txProducerFactory") ProducerFactory<String, Object> pf) {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(pf);
        kafkaTemplate.setObservationEnabled(true);
        kafkaTemplate.setObservationConvention(producerTemplateTracing);
        return kafkaTemplate;
    }

    /**
     * Kafka Transaction Manager
     */
    @Bean(name = "kafkaTransactionManager")
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(@Qualifier("txProducerFactory") ProducerFactory<String, Object> pf) {
        return new KafkaTransactionManager<>(pf);
    }

    @Bean(name = "transactionManager")
    public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }
}
