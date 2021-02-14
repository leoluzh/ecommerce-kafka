package com.lambdasys.ecommerce.service;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.lambdasys.ecommerce.commons.CorrelationId;
import com.lambdasys.ecommerce.commons.dispatcher.KafkaDispatcher;

/**
 * Introducao ao fast delegate ... 
 * envie a resposta rapidamente para o usuario e delege a tarefa de execução para outro servico.  
 * Objetivo é não travar usuario para ele não força um f5 ou se prevenir falhas ter que executar tudo novamente.
 * 
 * @author leoluzh
 *
 */

@SuppressWarnings("serial")
public class GenerateAllReportServlet extends HttpServlet {

	private static final String TOPIC_USER_GENERATE_READING_REPORT = "ECOMMERCE_USER_GENERATE_READING_REPORT";
	private static final String TOPIC_SEND_MESSAGE_TO_ALL_USERS = "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS";
	private final KafkaDispatcher<String>  batchDispatcher = new KafkaDispatcher<>();
	
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {

        	//reports will be generate um queued way ...
        	batchDispatcher.send(
        			TOPIC_SEND_MESSAGE_TO_ALL_USERS, 
        			TOPIC_USER_GENERATE_READING_REPORT , 
        			new CorrelationId( GenerateAllReportServlet.class.getSimpleName() ),
        			TOPIC_USER_GENERATE_READING_REPORT);
   
            System.out.println("Sent generated report to all users");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Reports request generated");

        } catch (Exception e) {
            throw new ServletException(e);
		}
        
    }	
	
	@Override
	public void destroy() {
		super.destroy();
		batchDispatcher.close();
	}
		
	
	
}
