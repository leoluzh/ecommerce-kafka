package com.lambdasys.ecommerce.commons;

public class Message<T> {

	private CorrelationId id;
	private T payload;
	
	public Message( CorrelationId id , T payload ) {
		this.id = id;
		this.payload = payload;
	}
	
	public CorrelationId getId() {
		return this.id;
	}
	
	public T getPayload() {
		return this.payload;
	}

	@Override
	public String toString() {
		return "Message [id=" + id + ", payload=" + payload + "]";
	}
	
}
