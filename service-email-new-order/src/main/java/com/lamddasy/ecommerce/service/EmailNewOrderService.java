package com.lamddasy.ecommerce.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.consumer.ConsumerService;
import com.lambdasys.ecommerce.commons.consumer.ServiceRunner;
import com.lambdasys.ecommerce.commons.dispatcher.KafkaDispatcher;

public class EmailNewOrderService implements ConsumerService<Order> {

	private static final String TOPIC_ECOMMERCE_NEW_ORDER = "ECOMMERCE_NEW_ORDER";
	private static final String TOPIC_ECOMMERCE_SEND_EMAIL = "ECOMMERCE_SEND_EMAIL" ;
	private static final Integer NUMBER_OF_THREADS = 5 ;
	
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

	public static void main( String[] args ) throws Exception {
		new ServiceRunner<>( EmailNewOrderService::new ).start( NUMBER_OF_THREADS );
	}

	public String getTopic() {
		return TOPIC_ECOMMERCE_NEW_ORDER;
	}
	
	public String getConsumerGroup() {
		return EmailNewOrderService.class.getSimpleName();
	}
	
    public void parse(ConsumerRecord<String, Message<Order>> record) throws Exception {
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, preparing email");
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
        var emailCode = "Thank you for your order! We are processing your order!" ;
        var message = record.value(); 
        var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        
        var order = record.value().getPayload();
        emailDispatcher.send(
        		TOPIC_ECOMMERCE_SEND_EMAIL, 
        		order.getEmail() ,
        		id ,  
        		emailCode );

    }
		
}
