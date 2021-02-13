package com.lambdasys.ecommerce.commons;

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

public class KafkaService<T> implements Closeable {

	private final KafkaConsumer<String,T> consumer;
	@SuppressWarnings("rawtypes")
	private final ConsumerFunction parse;
	
	@SuppressWarnings("rawtypes")
	public KafkaService( String groupId , String topic , ConsumerFunction parse , Class<T> type , Map<String,String> properties ) {
		this(parse,groupId,type,properties);
		consumer.subscribe(Collections.singletonList(topic));
	}

	@SuppressWarnings("rawtypes")
	public KafkaService( String groupId , Pattern topic , ConsumerFunction parse , Class<T> type , Map<String,String> properties ) {
		this(parse,groupId,type,properties);
		consumer.subscribe(topic);
	}
	
	@SuppressWarnings("rawtypes")
	private KafkaService( ConsumerFunction parse , String groupId , Class<T> type , Map<String,String> properties ) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<>( properties( type , groupId , properties ) );
	}
	
	@SuppressWarnings("unchecked")	
	public void run() {
		do {
			//call kafka to consume topic
			var records = consumer.poll(Duration.ofMillis(100));
			System.out.println( String.format("%s records found.", records.count() ));
			
			for( var record : records ) {
				try {
					parse.consume(record);
				}catch(Exception ex) {
					ex.printStackTrace();
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
	        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
	        properties.putAll(overrideProperties);
		}catch(Exception ex) {	
	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
	        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
	        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
	        properties.putAll(overrideProperties);
		}
        return properties;
    }
	
	
	@Override
	public void close() {
		consumer.close();
	}
	
}
