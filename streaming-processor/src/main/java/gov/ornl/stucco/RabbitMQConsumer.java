package gov.ornl.stucco;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

public class RabbitMQConsumer {
	private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

	private static final String EXCHANGE_TYPE = "topic";

	private String exchangeName;
	private String queueName;
	private String host;
	private int port;
	private String username;
	private String password;
	private String[] bindingKeys;
	private Channel channel;
	
	public RabbitMQConsumer(String exchangeName, String queueName, String host, int port, String username, String password, String[] bindingKeys) {
		this.exchangeName = exchangeName;
		this.queueName = queueName;
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.bindingKeys = bindingKeys;
	}
	
	public void openQueue() {
		//setup a connection
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		factory.setPort(port);
		if (username != null) {
			factory.setUsername(username);
		}
		if (password != null) {
			factory.setPassword(password);
		}
		
		try {
			Connection connection = factory.newConnection();
			//create a durable exchange on the channel
			channel = connection.createChannel();
			channel.exchangeDeclare(exchangeName, EXCHANGE_TYPE, true);
			//create a queue to consume messages with specific routing keys
			channel.queueDeclare(queueName, true, false, false, null);
			for (String key : bindingKeys) {
				channel.queueBind(queueName, exchangeName, key);
			}
		} catch (IOException e) {
			logger.error("Error creating spout connection.", e);
			System.exit(1);
		}
	}
	
	public GetResponse getMessage() {
		GetResponse response = null;
		try {
			response = channel.basicGet(queueName, false);
		} catch (IOException e) {
			logger.error("Error getting message from queue '" + queueName + "'.", e);
		}
		
		return response;
	}
	
	public void messageProcessed(long deliveryTag) {
		try {
			channel.basicAck(deliveryTag, false);
		} catch (IOException e) {
			logger.error("Error sending ack to data publisher.", e);
		}
	}

	public void retryMessage(long deliveryTag) {
		try {
			channel.basicNack(deliveryTag, false, true);
		} catch (IOException ex) {
			logger.error("Error sending nack to data publisher.", ex);
		}
	}
	
	public void close() {
		if ((channel != null) && (channel.getConnection() != null) && (channel.getConnection().isOpen())) {
			try {
				channel.getConnection().close();
			} catch (IOException e) {
				logger.error("Error closing connection.", e);
			}
		}
	}
}
