package com.lambdasys.ecommerce.commons.consumer;

import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class ServiceRunner<T> {
	
	private ServiceProvider<T> provider; 
	
	public ServiceRunner( ServiceFactory<T>  factory ) throws Exception {
		this.provider = new ServiceProvider<>(factory);
	}
	
	public void start( int threads ) {
		var pool = Executors.newFixedThreadPool( threads );
		IntStream.rangeClosed(1, threads).forEach( ( index ) -> {
			pool.submit( provider );
		});
	}

}
