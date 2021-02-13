package com.lambdasys.ecommerce.commons;

import java.io.Closeable;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaDispatcher<T> implements Closeable {
	
	private final KafkaProducer<String,T> producer;
	
	public KafkaDispatcher(){
		this.producer = new KafkaProducer<>( properties() );
	}
	
	private static Properties properties() {
		var properties = new Properties();
		try {
			properties.load(KafkaDispatcher.class.getClassLoader().getResourceAsStream("application.properties"));
		}catch(Exception ex) {	
	        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
	        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
	        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		}
		return properties;
	}
	
	public void send( String topic , String key , T value ) throws Exception {
		var record = new ProducerRecord<>(topic, key , value);
		Callback callback = ( data , ex ) -> {
			if( ex != null ) {
				ex.printStackTrace();
				return;
			}
			System.out.println( String.format("Sent sucessfully %s :::partition %s / offset %s / timestamp %s" , 
					data.topic() , 
					data.partition() , 
					data.offset() , 
					data.timestamp() ));
		};
		producer.send(record,callback).get();
	}
	
	@Override
	public void close() {
		producer.close();
	}
	
}
