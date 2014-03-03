package gov.ornl.stucco.spout;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

public class RabbitMQTopicSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQTopicSpout.class);
	
	private static final String EXCHANGE_TYPE = "topic";
	
	private SpoutOutputCollector collector;
	private Connection connection;
	private Channel channel;
	private String queue;
	private String exchangeName;
	private String queueName;
	private String host;
	private int port;
	private String username;
	private String password;
	private String[] bindingKeys;
	
	public RabbitMQTopicSpout(String exchangeName, String queueName, String host, int port, String username, String password, String[] bindingKeys) {
		this.exchangeName = exchangeName;
		this.queueName = queueName;
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.bindingKeys = bindingKeys;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		
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
			connection = factory.newConnection();
			//create a durable exchange on the channel
			channel = connection.createChannel();
			channel.exchangeDeclare(exchangeName, EXCHANGE_TYPE, true);
			//create a queue to consume messages with specific routing keys
			queue = channel.queueDeclare(queueName, true, false, false, null).getQueue();
			for (String key : bindingKeys) {
				channel.queueBind(queue, exchangeName, key);
			}
		} catch (IOException e) {
			logger.error("Error creating spout connection.", e);
			System.exit(1);
		}
		
	}

	@Override
	public void nextTuple() {
		String routingKey = null;
		String message = null;
		long timestamp = 0L;
		boolean contentIncluded = true;
		
		try {
			GetResponse response = channel.basicGet(queue, false);
			if (response != null) {
				message = new String(response.getBody());
				routingKey = response.getEnvelope().getRoutingKey();
				timestamp = response.getProps().getTimestamp().getTime();
				Map<String, Object> headerMap = response.getProps().getHeaders();
				if ((headerMap != null) && (headerMap.containsKey("content"))) {
					contentIncluded = Boolean.valueOf(String.valueOf(headerMap.get("content")));
				}
				
				long deliveryTag = response.getEnvelope().getDeliveryTag();
				if (message != null) {
					Values values = new Values(routingKey, timestamp, contentIncluded, message);
					logger.debug("emitting " + routingKey + " message- "+ values);
					collector.emit(values, deliveryTag);
				}
			}
			else {
				Utils.sleep(10);
			}
		} catch (Exception e) {
			logger.error("Error retrieving message from queue.", e);
		}
		
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("source", "timestamp", "contentIncl", "message"));
	}
	
	@Override
	public void ack(final Object id) {
		try {
			channel.basicAck((long) id, false);
		} catch(Exception ex) {
			logger.error("Error sending ack to data publisher.", ex);
		}
	}

	@Override
	public void fail(final Object id) {
		try {
			channel.basicNack((long) id, false, true);
		} catch(Exception ex) {
			logger.error("Error sending nack to data publisher.", ex);
		}
	} 
	
	public void close() {
		if (connection != null) {
			try {
				connection.close();
			} catch (Exception ex) {
				logger.error("Error closing spout connection.", ex);
			}
		}
	}

}
