package com.lambdasys.ecommerce.commons.consumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.dispatcher.GsonSerializer;
import com.lambdasys.ecommerce.commons.dispatcher.KafkaDispatcher;

public class KafkaService<T> implements Closeable {

	private static final String TOPIC_ECOMMERCE_DEADLETTER = "ECOMMERCE_DEADLETTER";
	private final KafkaConsumer<String,Message<T>> consumer;
	private final KafkaDispatcher<byte[]> deadLetterDispatcher = new KafkaDispatcher<>();
	private final GsonSerializer<Message<?>> jsonSerializer = new GsonSerializer<>(); 
	
	@SuppressWarnings("rawtypes")
	private final ConsumerFunction parse;
	
	@SuppressWarnings("rawtypes")
	public KafkaService( String groupId , String topic , ConsumerFunction<T> parse , Class<T> type , Map<String,String> properties ) {
		this(parse,groupId,type,properties);
		consumer.subscribe(Collections.singletonList(topic));
	}

	@SuppressWarnings("rawtypes")
	public KafkaService( String groupId , Pattern topic , ConsumerFunction<T> parse , Class<T> type , Map<String,String> properties ) {
		this(parse,groupId,type,properties);
		consumer.subscribe(topic);
	}
	
	@SuppressWarnings("rawtypes")
	private KafkaService( ConsumerFunction<T> parse , String groupId , Class<T> type , Map<String,String> properties ) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<>( properties( type , groupId , properties ) );
	}
	
	@SuppressWarnings("unchecked")	
	public void run() throws Exception {
		do {
			//call kafka to consume topic
			var records = consumer.poll(Duration.ofMillis(100));
			System.out.println( String.format("%s records found.", records.count() ));
			
			for( var record : records ) {
				try {
					parse.consume(record);
				}catch(Exception ex) {
					ex.printStackTrace();
					var message = record.value();
					deadLetterDispatcher.send(
							TOPIC_ECOMMERCE_DEADLETTER, 
							message.getId().toString() ,  
							message.getId().continueWith("DeadLetter"), 
							jsonSerializer.serialize("",record.value()));
				}
			}
						
		}while( true );
	}
	
    private Properties properties(Class<T> type, String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();
		try {
			properties.load(KafkaDispatcher.class.getClassLoader().getResourceAsStream("application.properties"));
	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
	        //number of off-set readings per poll/fetch in a topic.
	        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1");
	        properties.putAll(overrideProperties);
		}catch(Exception ex) {	
	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
	        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
	        properties.putAll(overrideProperties);
		}
        return properties;
    }
	
	
	@Override
	public void close() {
		consumer.close();
		deadLetterDispatcher.close();
		jsonSerializer.close();
	}
	
}
