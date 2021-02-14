package com.lambdasys.ecommerce.commons;

import java.io.Serializable;
import java.util.UUID;

@SuppressWarnings("serial")
public class CorrelationId implements Serializable {

	private final String id;
	
	public CorrelationId( String title ) {
		this.id = title + "(" + UUID.randomUUID().toString() + ")";
	}

	public String getId() {
		return this.id;
	}

	@Override
	public String toString() {
		return "CorrelationId [id=" + id + "]";
	}
	
	public CorrelationId continueWith( String title ) {
		return new CorrelationId( id + "-" + title );
	}
		
}
