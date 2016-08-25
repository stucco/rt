package gov.ornl.stucco;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.GetResponse;

public class RabbitMQMessage {
	private static final Logger logger = LoggerFactory.getLogger(RabbitMQMessage.class);

	GetResponse response;

	public RabbitMQMessage(GetResponse response){
		this.response = response;
	}

	public String getRoutingKey(){
		String key = response.getEnvelope().getRoutingKey();
		if(key == null){
			logger.warn("Unexpected null routing key");
			key = "NULL";
		}
		return key;
	}

	public long getId(){
		return response.getEnvelope().getDeliveryTag();
	}

	public long getTimestamp(){
		long timestamp = 0;
		if (response.getProps().getTimestamp() != null) {
			timestamp = response.getProps().getTimestamp().getTime();
		}
		return timestamp;
	}

	public Map<String, Object> getHeaders(){
		Map<String, Object> headerMap = response.getProps().getHeaders();
		return headerMap;
	}

	public String getBody(){
		byte[] body = response.getBody();
		if(body == null){
			return null;
		}
		else{
			return new String(body);
		}
	}
}
