package com.lambdasys.ecommerce.service;

import java.io.Closeable;
import java.io.IOException;

import com.lambdasys.ecommerce.database.LocalDataBase;

public class OrdersDataBase implements Closeable {

	private LocalDataBase localdatabase;
	
	public OrdersDataBase() throws Exception {
		this.localdatabase = new LocalDataBase("orders_database");
		this.localdatabase.createIfNotExists("CREATE TABLE ORDERS ( uuid VARCHAR(200) PRIMARY KEY )");
	}
	
	public boolean saveNew( Order order ) throws Exception {
		if( wasProcessed(order) ) {
			return false;
		}		
		this.localdatabase.update("INSERT INTO ORDERS ( uuid ) VALUES ( ? ) ",  order.getOrderId() );
		return true;
	}
	
	public boolean wasProcessed( Order order ) throws Exception {
		var result = this.localdatabase.query("SELECT uuid FROM ORDERS WHERE uuid = ? LIMIT 1", order.getOrderId() );
		return result.next();
	}

	@Override
	public void close() throws IOException {
		this.localdatabase.close();
	}
	
}
