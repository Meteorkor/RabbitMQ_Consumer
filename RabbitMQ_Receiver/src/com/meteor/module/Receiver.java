package com.meteor.module;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

public class Receiver {

	String host;
	String basic_queue_name = "basicQ";
	public Receiver(String host){
		
		set_host(host);
		
	}
	
	public void set_host(String addr){
		this.host = addr;
	}
	
	public void basic_receiver(){
		String message;
		ConnectionFactory cf= new ConnectionFactory();
		cf.setHost(host);
		
		try {
			Connection conn = cf.newConnection();
			Channel channel = conn.createChannel();
			channel.queueDeclare(basic_queue_name,false,false,false,null);
			System.out.println("Waiting for messages");
		
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume( basic_queue_name,true, consumer);
			
			while(true){
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				message = new String(delivery.getBody());
				System.out.println("Received : " + message);
				
				
			}
			
			
			
		} catch (IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}
