package gov.ornl.stucco;

import java.util.Map;

import com.rabbitmq.client.GetResponse;

public class RabbitMQMessage {

	GetResponse response;
	
	public RabbitMQMessage(GetResponse response){
		this.response = response;
	}
	
	public String getRoutingKey(){
		return response.getEnvelope().getRoutingKey();
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
