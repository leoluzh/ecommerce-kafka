package com.lambdasys.ecommerce.service;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.database.LocalDataBase;
import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.consumer.ConsumerService;
import com.lambdasys.ecommerce.commons.consumer.ServiceRunner;
import com.lambdasys.ecommerce.commons.dispatcher.KafkaDispatcher;

public class FraudDetectorService implements ConsumerService<Order> , Closeable {

	private static final String TOPIC_ECOMMERCE_ORDER_APPROVED = "ECOMMERCE_ORDER_APPROVED";
	private static final String TOPIC_ECOMMERCE_ORDER_REJECTED = "ECOMMERCE_ORDER_REJECTED";
	private static final String TOPIC_ECOMMERCE_NEW_ORDER = "ECOMMERCE_NEW_ORDER";
	private static final Integer NUMBER_OF_THREADS = 1 ;
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
	private LocalDataBase localdatabase;

	public static void main( String[] args ) throws Exception {
		new ServiceRunner<>(FraudDetectorService::new).start(NUMBER_OF_THREADS);
	}
	
	public FraudDetectorService() throws Exception {
		this.localdatabase = new LocalDataBase("frauds_detector_database");
		this.localdatabase.createIfNotExists("CREATE TABLE ORDERS ( uuid VARCHAR(200) PRIMARY KEY , is_fraud BOOLEAN )");
		
	}
	
	public String getTopic() {
		return TOPIC_ECOMMERCE_NEW_ORDER;
	}

	public String getConsumerGroup() {
		return FraudDetectorService.class.getSimpleName();
	}
	
	
    public void parse(ConsumerRecord<String, Message<Order>> record) throws Exception {
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        
        var order = record.value().getPayload();
        
        if( wasProcessed( order ) ) {
        	System.out.println("Order " + order.getOrderId() + " was already processed");
        	return;
        }
        
        if(isFraud(order)) {
        	//insert into database ...
        	localdatabase.update("INSERT INTO ORDERS ( uuid , is_fraud ) VALUES ( ? , true ) ", order.getOrderId() );
        	
            // pretending that the fraud happens when the amount is >= 4500
            System.out.println("Order is a fraud!!!!!" + order);
            orderDispatcher.send(TOPIC_ECOMMERCE_ORDER_REJECTED, order.getEmail(), record.value().getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        } else {
        	//insert into database ...
        	localdatabase.update("INSERT INTO ORDERS ( uuid , is_fraud ) VALUES ( ? , false ) ", order.getOrderId() );

        	System.out.println("Approved: " + order);
            orderDispatcher.send(TOPIC_ECOMMERCE_ORDER_APPROVED, order.getEmail(), record.value().getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }

    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
    
    private boolean wasProcessed( Order order ) throws SQLException {
    	ResultSet result = this.localdatabase.query("SELECT uuid FROM ORDERS WHERE uuid = ? LIMIT 1", order.getOrderId() );
    	return result.next();
    }

	@Override
	public void close() throws IOException {
		this.localdatabase.close();
	}
		
}
