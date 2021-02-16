package com.lambdasys.ecommerce.commons.consumer;

public interface ServiceFactory<T> {
	ConsumerService<T> create();
}
