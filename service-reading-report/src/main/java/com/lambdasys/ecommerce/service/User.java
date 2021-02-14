package com.lambdasys.ecommerce.service;

public class User {

	private final String uuid;
	
	public User( String uuid ) {
		this.uuid = uuid;
	}
	
	public String getUuid() {
		return this.uuid;
	}
	
	public String getReportPath() {
		return "target/" + uuid + "-report.txt" ;
	}
	
}
