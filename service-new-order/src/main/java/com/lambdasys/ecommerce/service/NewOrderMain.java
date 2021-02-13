package com.lambdasys.ecommerce.service;

import java.math.BigDecimal;
import java.util.UUID;

import com.lambdasys.ecommerce.commons.KafkaDispatcher;

public class NewOrderMain {

    private static final String TOPIC_ECOMMERCE_SEND_EMAIL = "ECOMMERCE_SEND_EMAIL";
	private static final String TOPIC_ECOMMERCE_NEW_ORDER = "ECOMMERCE_NEW_ORDER";

	public static void main(String[] args) throws Exception {
        try (var orderDispatcher = new KafkaDispatcher<Order>();
             var emailDispatcher = new KafkaDispatcher<String>()) {
                var email = Math.random() + "@email.com";
                for (var i = 0; i < 10; i++) {

                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);

                    var order = new Order(orderId, amount, email);
                    orderDispatcher.send(TOPIC_ECOMMERCE_NEW_ORDER, email, order);

                    var emailCode = "Thank you for your order! We are processing your order!";
                    emailDispatcher.send(TOPIC_ECOMMERCE_SEND_EMAIL, email, emailCode);
                }
        }
    }
}	
	

