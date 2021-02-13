package com.lambdasys.ecommerce.service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.KafkaDispatcher;
import com.lambdasys.ecommerce.commons.KafkaService;

public class FraudDetectorService {

	private static final String TOPIC_ECOMMERCE_ORDER_APPROVED = "ECOMMERCE_ORDER_APPROVED";
	private static final String TOPIC_ECOMMERCE_ORDER_REJECTED = "ECOMMERCE_ORDER_REJECTED";
	private static final String TOPIC_ECOMMERCE_NEW_ORDER = "ECOMMERCE_NEW_ORDER";
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

	public static void main( String[] args ) {
		var fraudService = new FraudDetectorService();
        try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                TOPIC_ECOMMERCE_NEW_ORDER,
                fraudService::parse,
                Order.class,
                Map.of())) {
            service.run();
        }
	}

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
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
        var order = record.value();
        if(isFraud(order)) {
            // pretending that the fraud happens when the amount is >= 4500
            System.out.println("Order is a fraud!!!!!" + order);
            orderDispatcher.send(TOPIC_ECOMMERCE_ORDER_REJECTED, order.getEmail(), order);
        } else {
            System.out.println("Approved: " + order);
            orderDispatcher.send(TOPIC_ECOMMERCE_ORDER_APPROVED, order.getEmail(), order);
        }

    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
	
	
}
