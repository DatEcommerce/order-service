package com.dat.backend.orderservice.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class ConsumerConfig {

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProperties());
    }

    @Bean
    public java.util.Map<String, Object> consumerProperties() {
        java.util.Map<String, Object> props = new java.util.HashMap<>();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return props;
    }

    @Bean
    public DefaultErrorHandler errorHandler(@Qualifier("nonTxKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                ((record, e) -> new TopicPartition(
                        record.topic() + "-dlt",
                        record.partition()
                ))
        );
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }

    @Bean
    public ConsumerFactory<String, Object> txConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(txConsumerProperties());
    }

    @Bean
    public java.util.Map<String, Object> txConsumerProperties() {
        Map<String, Object> props = consumerProperties();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return props;
    }

    /**
     * Batch listener container factory
     */
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> batchKafkaListenerContainerFactory(
            @Qualifier("nonTxKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(2);
        factory.setBatchListener(true);
        factory.setBatchMessageConverter(new BatchMessagingMessageConverter(new JsonMessageConverter()));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH); // Set ack mode to MANUAL_IMMEDIATE
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                // Handle partition revocation before committing offsets
                List<String> partitionList = partitions.stream()
                        .map(TopicPartition::partition)
                        .map(String::valueOf)
                        .toList();
                String consumerId = consumer.groupMetadata().memberId();
                String partitionIds = String.join(", ", partitionList);
                // TODO: send to monitoring service
                log.info("Consumer: {} - revoking partitions: {}", consumerId, partitionIds);
            }

            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.info("Partitions revoked after commit: {}", partitions);
            }

            @Override
            public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.warn("Partitions lost: {}", partitions);
            }
        });
        factory.getContainerProperties().setMicrometerEnabled(true);
        factory.getContainerProperties().setIdleEventInterval(60000L); // Set the interval to publish an IdleContainerEvent if no records are received. The listener can capture this event to perform some action when the container is idle.
        factory.getContainerProperties().setNoPollThreshold(2);
        factory.getContainerProperties().setPollTimeout(2000L);
        factory.setCommonErrorHandler(errorHandler(kafkaTemplate));
        return factory;
    }

    /**
     * Listener container factory for transaction consumer
     */
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> transactionalKafkaListenerContainerFactory(
            @Qualifier("kafkaTransactionManager") KafkaTransactionManager<String, Object> kafkaTransactionManager,
            @Qualifier("txKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(txConsumerFactory());
        factory.setConcurrency(1);
        factory.setBatchListener(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setKafkaAwareTransactionManager(kafkaTransactionManager);
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                ((record, e) -> new TopicPartition(
                        record.topic() + "-dlt",
                        record.partition()
                ))
        );
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000);
        backOff.setMultiplier(2);
        backOff.setMaxInterval(5000);
        DefaultAfterRollbackProcessor<String, Object> processor =
                new DefaultAfterRollbackProcessor<>(recoverer, backOff, kafkaTemplate, true);
        factory.setAfterRollbackProcessor(processor);
        return factory;
    }

    /**
     * Listener container factory for DLT listener
     */
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>> kafkaListenerContainerFactory(
            @Qualifier("nonTxKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(1);
        factory.setBatchListener(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD); // Set ack mode to MANUAL_IMMEDIATE
        factory.getContainerProperties().setMicrometerEnabled(true);
        factory.getContainerProperties().setIdleEventInterval(60000L); // Set the interval to publish an IdleContainerEvent if no records are received. The listener can capture this event to perform some action when the container is idle.
        factory.getContainerProperties().setNoPollThreshold(2);
        factory.getContainerProperties().setPollTimeout(2000L);
        return factory;
    }
}
