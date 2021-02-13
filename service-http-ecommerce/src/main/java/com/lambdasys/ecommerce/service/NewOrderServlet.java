package com.lambdasys.ecommerce.service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.lambdasys.ecommerce.commons.KafkaDispatcher;

@SuppressWarnings("serial")
public class NewOrderServlet extends HttpServlet {

	private static final String TOPIC_ECOMMERCE_SEND_EMAIL = "ECOMMERCE_SEND_EMAIL";
	private static final String TOPIC_ECOMMERCE_NEW_ORDER = "ECOMMERCE_NEW_ORDER";
	private final KafkaDispatcher<Order>  orderDispatcher = new KafkaDispatcher<>();
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();
	
  @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {

            // we are not caring about any security issues, we are only
            // showing how to use http as a starting point
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));

            var orderId = UUID.randomUUID().toString();

            var order = new Order(orderId, amount, email);
            orderDispatcher.send(TOPIC_ECOMMERCE_NEW_ORDER, email, order);

            var emailCode = "Thank you for your order! We are processing your order!";
            emailDispatcher.send(TOPIC_ECOMMERCE_SEND_EMAIL, email, emailCode);

            System.out.println("New order sent successfully.");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New order sent");

        } catch (Exception e) {
            throw new ServletException(e);
		}
        
    }	
	
	@Override
	public void destroy() {
		super.destroy();
		orderDispatcher.close();
		emailDispatcher.close();
	}
	
}