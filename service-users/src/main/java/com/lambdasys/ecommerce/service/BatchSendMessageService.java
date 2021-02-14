package com.lambdasys.ecommerce.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.CorrelationId;
import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.consumer.KafkaService;
import com.lambdasys.ecommerce.commons.dispatcher.KafkaDispatcher;

public class BatchSendMessageService {

    private static final String TOPIC_SEND_MESSAGE_TO_ALL_USERS = "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS";
	private final Connection connection;
    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    public BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("CREATE TABLE USERS (" +
                    "uuid VARCHAR(200) PRIMARY KEY ," +
                    "email VARCHAR(200))");
        } catch(SQLException ex) {
            // be careful, the sql could be wrong, be reallllly careful
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                TOPIC_SEND_MESSAGE_TO_ALL_USERS,
                batchService::parse,
                String.class,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws Exception {
        System.out.println("------------------------------------------");
        System.out.println("Processing new batch");
        var message = record.value();
        System.out.println("Topic: " + message.getPayload() );
        
        for( var user : getUsers() ) {
        	userDispatcher.sendAsync( message.getPayload() , user.getUuid() , message.getId().continueWith(BatchSendMessageService.class.getSimpleName()) , user);
        }
        
    }

	private List<User> getUsers() throws SQLException{
		ResultSet result = connection.prepareStatement("SELECT uuid FROM USERS").executeQuery();
		List<User> users = new ArrayList<>();
		while( result.next() ) {
			users.add( new User( result.getString(1) ) );
		}
		return users;
	}
	
}
