package com.lambdasys.ecommerce.commons.consumer;

import java.util.Map;
import java.util.concurrent.Callable;

public class ServiceProvider<T> implements Callable<Void> {
	
	private  ServiceFactory<T> factory;
	
	public ServiceProvider(  ServiceFactory<T> factory ) throws Exception {
		this.factory = factory;
	}

	public Void call() throws Exception {
		var service = factory.create();
		try( var kservice = new KafkaService<>(  
				service.getConsumerGroup() , 
				service.getTopic() , 
				service::parse , 
				Map.of()) ){
			
		}
		return null;
	}
	
}
