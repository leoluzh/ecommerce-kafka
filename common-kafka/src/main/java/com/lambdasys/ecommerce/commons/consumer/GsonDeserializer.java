package com.lambdasys.ecommerce.commons.consumer;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.MessageAdapter;


@SuppressWarnings("rawtypes")
public class GsonDeserializer implements Deserializer<Message> {

	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class,MessageAdapter.class).create();
	
	@Override
	public Message deserialize( String s , byte[] bytes ) {
		return gson.fromJson( new String(bytes) , Message.class);
	}
	
}
