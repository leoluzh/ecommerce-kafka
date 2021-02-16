package com.lambdasys.ecommerce.service;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.consumer.ConsumerService;
import com.lambdasys.ecommerce.commons.consumer.ServiceRunner;
import com.lambdasys.ecommerce.commons.dispatcher.KafkaDispatcher;
import com.lambdasys.ecommerce.database.LocalDataBase;

public class BatchSendMessageService implements ConsumerService<String> , Closeable {

    private static final String TOPIC_SEND_MESSAGE_TO_ALL_USERS = "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS";
    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();
    private static final Integer NUMBER_OF_THREADS = 5;
    private LocalDataBase localdatabase;

    public BatchSendMessageService() throws Exception{
    	this.localdatabase = new LocalDataBase("users_database");
    	this.localdatabase.createIfNotExists("CREATE TABLE USERS (" +
                " uuid VARCHAR(200) PRIMARY KEY , " +
                " email VARCHAR(200) )");
    }

    public static void main(String[] args) throws Exception {
    	new ServiceRunner<>(BatchSendMessageService::new).start(NUMBER_OF_THREADS);
    }
    
    public String getTopic() {
    	return TOPIC_SEND_MESSAGE_TO_ALL_USERS;
    }

    public String getConsumerGroup() {
    	return BatchSendMessageService.class.getSimpleName();
    }
    
    public void parse(ConsumerRecord<String, Message<String>> record) throws Exception {
        System.out.println("------------------------------------------");
        System.out.println("Processing new batch");
        var message = record.value();
        System.out.println("Topic: " + message.getPayload() );
        
        for( var user : getUsers() ) {
        	userDispatcher.sendAsync( message.getPayload() , user.getUuid() , message.getId().continueWith(BatchSendMessageService.class.getSimpleName()) , user);
        }
        
    }

	private List<User> getUsers() throws SQLException{
		ResultSet result = this.localdatabase.query("SELECT uuid FROM USERS");
		List<User> users = new ArrayList<>();
		while( result.next() ) {
			users.add( new User( result.getString(1) ) );
		}
		return users;
	}

	@Override
	public void close() throws IOException {
		this.localdatabase.close();
	}

	
}
