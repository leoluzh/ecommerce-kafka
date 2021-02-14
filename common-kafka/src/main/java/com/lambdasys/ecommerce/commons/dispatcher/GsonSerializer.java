package com.lambdasys.ecommerce.commons.dispatcher;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.MessageAdapter;



public class GsonSerializer<T> implements Serializer<T> {
	
	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter() ).create();
	
	@Override
	public byte[] serialize( String s , T object ){
		return gson.toJson(object).getBytes();
	}	
	
}
