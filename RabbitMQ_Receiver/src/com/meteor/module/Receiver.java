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
	String durable_queue_name = "durableQ";
	
	String fanout_queue_name = "fanout_durableQ";
	
	
	int prefetchCount = 10;
	/*
	In order to defeat that we can use the basicQos method with 
	the prefetchCount = 1 setting. 
	This tells RabbitMQ not to give more than one message to a worker at a time. 
	Or, in other words, don't dispatch a new message to a worker 
	until it has processed and acknowledged the previous one. 
	Instead, it will dispatch it to the next worker that is not still busy.
	
	int prefetchCount = 1;
	channel.basicQos(prefetchCount);
	
	*/
	
	
	boolean durable = true;
	//boolean durable = false;
	/*
	 When RabbitMQ quits or crashes it will forget the queues 
	 and messages unless you tell it not to. 
	 Two things are required to make sure that messages aren't lost: 
	 we need to mark both the queue and messages as durable.
	*/
	//Boolean autoAck = false;
	Boolean autoAck = true;
	/*
	If a consumer dies without sending an ack, 
	RabbitMQ will understand that a message wasn't processed fully 
	and will redeliver it to another consumer.
	That way you can be sure that no message is lost, 
	even if the workers occasionally die.

	There aren't any message timeouts; 
	RabbitMQ will redeliver the message only when the worker connection dies. 
	It's fine even if processing a message takes a very, very long time.

	Message acknowledgments are turned on by default. 
	In previous examples we explicitly turned them off 
	via the autoAck=true flag. 
	It's time to remove this flag and send a proper acknowledgment 
	from the worker, once we're done with a task.
	
	Using this code we can be sure that even if you kill a worker using 
	CTRL+C while it was processing a message, nothing will be lost. 
	Soon after the worker dies all unacknowledged messages will be redelivered.
	*/
	//channel.queueBind(qn, "di_logs", "critical");
	/*
	channel.queueBind(qn, "di_logs", "critical");
	"critical" Àº Serverity
	
	*/
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
	public void acknowledged_receiver(){
		String message;
		ConnectionFactory cf= new ConnectionFactory();
		cf.setHost(host);
		
		try {
			Connection conn = cf.newConnection();
			Channel channel = conn.createChannel();
			
			
			
			channel.queueDeclare(basic_queue_name,durable,false,false,null);
			System.out.println("Waiting for messages");
			
			QueueingConsumer consumer = new QueueingConsumer(channel);
			
			channel.basicConsume( basic_queue_name, autoAck, consumer);
			
			//while(true){
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				message = new String(delivery.getBody());
				System.out.println("Received : " + message);
				/*
				System.out.println(delivery.getEnvelope());
				System.out.println(delivery.getEnvelope().getDeliveryTag());
				System.out.println(delivery.getEnvelope().getExchange());
				System.out.println(delivery.getProperties());
				*/
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				
			
			System.out.println("out");
			channel.close();
			conn.close();
			
			
		} catch (IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void durable_acknowledged_receiver(){
		String message;
		ConnectionFactory cf= new ConnectionFactory();
		cf.setHost(host);
		
		try {
			Connection conn = cf.newConnection();
			Channel channel = conn.createChannel();
			
			channel.queueDeclare(durable_queue_name,durable,false,false,null);
			System.out.println("Waiting for messages");
			
			QueueingConsumer consumer = new QueueingConsumer(channel);
			
			channel.basicQos(prefetchCount);
			
			channel.basicConsume( durable_queue_name, autoAck, consumer);
	
			//while(true){}
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				message = new String(delivery.getBody());
				System.out.println("Received : " + message);
				/*
				System.out.println(delivery.getEnvelope());
				System.out.println(delivery.getEnvelope().getDeliveryTag());
				System.out.println(delivery.getEnvelope().getExchange());
				System.out.println(delivery.getProperties());
				*/
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			
			
		} catch (IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	

	public void fanout_durable_acknowledged_receiver(){
		String message;
		ConnectionFactory cf= new ConnectionFactory();
		cf.setHost(host);
		
		try {
			Connection conn = cf.newConnection();
			Channel channel = conn.createChannel();
			
			//channel.queueDeclare(fanout_queue_name,durable,false,false,null);
			System.out.println("Waiting for messages");
			
			QueueingConsumer consumer = new QueueingConsumer(channel);
			
			channel.basicQos(prefetchCount);
			
			
			channel.exchangeDeclare("logs", "fanout");
			String qn = channel.queueDeclare().getQueue();
			channel.queueBind(qn, "logs", "aaa");
			
			channel.basicConsume( qn, autoAck, consumer);
	
			while(true){
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				message = new String(delivery.getBody());
				System.out.println("Received : " + message);
				/*
				System.out.println(delivery.getEnvelope());
				System.out.println(delivery.getEnvelope().getDeliveryTag());
				System.out.println(delivery.getEnvelope().getExchange());
				System.out.println(delivery.getProperties());
				*/
				//channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			System.out.println(delivery.getEnvelope().getRoutingKey());
			}
			
		} catch (IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	public void direct_durable_acknowledged_receiver(){
		String message;
		ConnectionFactory cf= new ConnectionFactory();
		cf.setHost(host);
		
		try {
			Connection conn = cf.newConnection();
			Channel channel = conn.createChannel();
			
			//channel.queueDeclare(fanout_queue_name,durable,false,false,null);
			System.out.println("Waiting for messages");
			
			QueueingConsumer consumer = new QueueingConsumer(channel);
			
			channel.basicQos(prefetchCount);

			
			channel.exchangeDeclare("di_logs", "direct");
			String qn = channel.queueDeclare().getQueue();
			channel.queueBind(qn, "di_logs", "critical");
			//Serverity
			channel.basicConsume( qn, autoAck, consumer);
	
			while(true){
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				message = new String(delivery.getBody());
				System.out.println("Received : " + message);
				
				System.out.println(delivery.getEnvelope().getRoutingKey());
			}
			
		} catch (IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	public void topic_durable_acknowledged_receiver(){
		String message;
		ConnectionFactory cf= new ConnectionFactory();
		cf.setHost(host);
		
		try {
			Connection conn = cf.newConnection();
			Channel channel = conn.createChannel();
			
			//channel.queueDeclare(fanout_queue_name,durable,false,false,null);
			System.out.println("Waiting for messages");
			
			QueueingConsumer consumer = new QueueingConsumer(channel);
			
			channel.basicQos(prefetchCount);

			
			channel.exchangeDeclare("topic_logs", "topic");
			String qn = channel.queueDeclare().getQueue();
			//channel.queueBind(qn, "topic_logs", "critical");
			channel.queueBind(qn, "topic_logs", "kim.i.*");
			//Serverity
			channel.basicConsume( qn, autoAck, consumer);
	
			while(true){
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				message = new String(delivery.getBody());
				System.out.println("Received : " + message);
				
				System.out.println(delivery.getEnvelope().getRoutingKey());
			}
			
		} catch (IOException | ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}
