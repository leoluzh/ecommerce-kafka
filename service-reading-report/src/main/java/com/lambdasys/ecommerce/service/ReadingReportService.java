package com.lambdasys.ecommerce.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lambdasys.ecommerce.commons.Message;
import com.lambdasys.ecommerce.commons.consumer.KafkaService;

public class ReadingReportService {

	private static final String TOPIC_USER_GENERATE_READING_REPORT = "ECOMMERCE_USER_GENERATE_READING_REPORT";
	private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();
	
	public static void main( String[] args ) throws Exception {
		var reportService = new ReadingReportService();
		try( var service = new KafkaService<>(
				ReadingReportService.class.getSimpleName() , 
				TOPIC_USER_GENERATE_READING_REPORT ,
				reportService::parse ,
				User.class , 
				Map.of())){
			service.run();
		}
	}
	
    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("------------------------------------------");
        System.out.println("Processing report for " + record.value());

        var user = record.value().getPayload();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());

        System.out.println("File created: " + target.getAbsolutePath());

    }	
	
}
