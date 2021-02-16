package com.lambdasys.ecommerce.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.consumer.ConsumerService;
import com.lambdasys.ecommerce.commons.consumer.ServiceRunner;

public class ReadingReportService implements ConsumerService<User> {

	private static final String TOPIC_USER_GENERATE_READING_REPORT = "ECOMMERCE_USER_GENERATE_READING_REPORT";
	private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();
	private static final Integer NUMBER_OF_THREADS = 5;
	
	public static void main( String[] args ) throws Exception {
		new ServiceRunner<>(ReadingReportService::new).start(NUMBER_OF_THREADS);
	}
	
	public String getTopic() {
		return TOPIC_USER_GENERATE_READING_REPORT;
	}
	
	public String getConsumerGroup() {
		return ReadingReportService.class.getSimpleName();
	}
	
    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("------------------------------------------");
        System.out.println("Processing report for " + record.value());

        var user = record.value().getPayload();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());

        System.out.println("File created: " + target.getAbsolutePath());

    }	
	
}
