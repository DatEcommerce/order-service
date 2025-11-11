package com.dat.backend.orderservice.tracing;

import io.micrometer.common.KeyValues;
import lombok.NonNull;
import org.springframework.kafka.support.micrometer.KafkaListenerObservationConvention;
import org.springframework.kafka.support.micrometer.KafkaRecordReceiverContext;
import org.springframework.stereotype.Component;

@Component
public class ConsumerTemplateTracing implements KafkaListenerObservationConvention {
    @Override
    public String getName() {
        return "kafka.consumer.template";
    }

    @Override
    @NonNull
    public KeyValues getLowCardinalityKeyValues(KafkaRecordReceiverContext context) {
        return KeyValues.of(
                "topic", context.getRecord().topic(),
                "key", String.valueOf(context.getRecord().key())
        );
    }
}
