package com.lambdasys.ecommerce.service;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.consumer.ConsumerService;
import com.lambdasys.ecommerce.commons.consumer.KafkaService;
import com.lambdasys.ecommerce.commons.consumer.ServiceRunner;
import com.lambdasys.ecommerce.database.LocalDataBase;

public class CreateUserService implements ConsumerService<Order> , Closeable {

    private static final String TOPIC_ECOMMERCE_NEW_ORDER = "ECOMMERCE_NEW_ORDER";
    private static final Integer NUMBER_OF_THREADS = 5 ;
	private LocalDataBase database;

    public CreateUserService() throws Exception {
    	this.database = new LocalDataBase("users_database");
    	this.database.createIfNotExists("CREATE TABLE USERS ( uuid varchar(200) PRIMARY KEY , email VARCHAR(200) )");
    }
    

    public static void main(String[] args) throws Exception {
    	new ServiceRunner<>(CreateUserService::new).start(NUMBER_OF_THREADS);
    }
    
    public String getTopic() {
    	return TOPIC_ECOMMERCE_NEW_ORDER;
    }
    
    public String getConsumerGroup() {
    	return CreateUserService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println(record.value());
        var order = record.value().getPayload();
        if(isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        String uuid = UUID.randomUUID().toString();
    	database.update("INSERT INTO USERS (uuid, email) " +
                "values (?,?)" , uuid , email );
        System.out.println("User with uuid: " + uuid  + " and email: " + email  + " created");
    }

    private boolean isNewUser(String email) throws SQLException {
    	var results = database.query("SELECT uuid FROM USERS WHERE email = ? LIMIT 1", email);
    	return !results.next();
    }


	@Override
	public void close() throws IOException {
		this.database.close();
	}	
    	
}
