package com.lambdasys.ecommerce.commons.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.Message;

public interface ConsumerFunction<T> {
    void consume( ConsumerRecord<String,Message<T>> record ) throws Exception;
}
