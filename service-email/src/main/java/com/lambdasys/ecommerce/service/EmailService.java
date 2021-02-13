package com.lambdasys.ecommerce.service;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.KafkaService;


public class EmailService {

	private static final String TOPIC_ECOMMERCE_SEND_EMAIL = "ECOMMERCE_SEND_EMAIL";

	public static void main( String[] args ) {
		var emailService = new EmailService();
		try( var service = new KafkaService(
				EmailService.class.getSimpleName() ,
				TOPIC_ECOMMERCE_SEND_EMAIL , 
				emailService::parse , 
				String.class , 
				Map.of()) ){
			service.run();
		}
	}
	
	private void parse( ConsumerRecord<String,String> record ) {
        System.out.println("------------------------------------------");
        System.out.println("Send email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        System.out.println("Email sent");		
	}
	
}
