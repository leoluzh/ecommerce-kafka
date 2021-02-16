
package com.lambdasys.ecommerce.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.consumer.ConsumerService;
import com.lambdasys.ecommerce.commons.consumer.ServiceRunner;


public class EmailService implements ConsumerService<String> {

	private static final String TOPIC_ECOMMERCE_SEND_EMAIL = "ECOMMERCE_SEND_EMAIL";
	//pay attetion - number of threads is related with the number of partitions in kafka topic.
	//number of consumers greather then partitions is a waste of resources.
	private static final Integer NUMBER_OF_THREADS = 5 ;

	public static void main( String[] args ) throws Exception {
		new ServiceRunner( EmailService::new ).start(NUMBER_OF_THREADS);
	}
	
	public String getTopic() {
		return TOPIC_ECOMMERCE_SEND_EMAIL;
	}
	
	public String getConsumerGroup() {
		return EmailService.class.getSimpleName();
	}
	
	public void parse( ConsumerRecord<String,Message<String>> record ) {
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
