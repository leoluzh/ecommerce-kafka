package com.lambdasys.ecommerce.commons.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.Message;

public interface ConsumerService<T> {
	void parse( ConsumerRecord<String,Message<T>> record ) throws Exception ;
	String getTopic();
	String getConsumerGroup();
}
