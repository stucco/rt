package gov.ornl.stucco;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import gov.pnnl.stucco.doc_service_client.DocServiceClient;
import gov.pnnl.stucco.doc_service_client.DocServiceException;

public class RabbitMQProducer {
	private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducer.class);

	private static final String EXCHANGE_TYPE = "topic";

	private String exchangeName;
	private String queueName;
	private String host;
	private int port;
	private String username;
	private String password;
	private String[] bindingKeys;
	private Channel channel;
	private int maxMessageSize;

	private DocServiceClient docServiceClient;

	public RabbitMQProducer(String exchangeName, String queueName, String host, int port, String username, String password, String[] bindingKeys) {
		this.exchangeName = exchangeName;
		this.queueName = queueName;
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.bindingKeys = bindingKeys;
		this.maxMessageSize = 10000000; //TODO: pass as arg?
	}

	public RabbitMQProducer(Map<String, Object> configMap) {
		String[] bindingKeys = null;

		this.exchangeName = String.valueOf(configMap.get("exchange"));
		this.queueName = String.valueOf(configMap.get("queue"));
		this.host = String.valueOf(configMap.get("host"));
		this.port = Integer.parseInt(String.valueOf(configMap.get("port")));
		this.username = String.valueOf(configMap.get("username"));
		this.password = String.valueOf(configMap.get("password"));

		@SuppressWarnings("unchecked")
		List<String> bindings = (List<String>)(configMap.get("bindings"));
		bindingKeys = new String[bindings.size()];
		bindingKeys = bindings.toArray(bindingKeys);
		this.bindingKeys = bindingKeys;

		this.maxMessageSize = 10000000; //TODO: pass as arg?

		logger.info("Created RabbitMQProducer with this info: \nhost: " + host + "\nport: " + port + 
				"\nexchange: " + exchangeName + "\nqueue: " + queueName + 
				"\nuser: " + username + "\npass: " + password);
	}

	public void openQueue() throws IOException {
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
			logger.error("Error creating spout connection.");
			throw e;
		}
	}

	public void close() throws IOException {
		if ((channel != null) && (channel.getConnection() != null) && (channel.getConnection().isOpen())) {
			try {
				channel.getConnection().close();
			} catch (IOException e) {
				logger.error("Error closing connection.");
				throw e;
			}
		}
	}

	/** 
     * Sends a content message. Depending on content size, this either sends
     * the content directly, or it first stores the document and sends the ID.
     *  
     * @deprecated 
     * Use {@link #sendIdMessage(Map, byte[])} instead. Retained for now for 
     * backward compatibility.
     */
    public void send(Map<String, String> metadata, byte[] rawContent) {
        // TODO: Refactor to reuse the connection/channel instead of creating
        // anew each time

        try {
            // Determine if the data should be sent straight to the queue or to the document service
            if (rawContent.length > maxMessageSize) {
                String docId = saveToDocumentStore(rawContent, metadata.get("contentType"));
                if (!docId.isEmpty()) {
                    sendIdMessage(metadata, docId.getBytes());
                }
            } else {
                sendContentMessage(metadata, rawContent);
            }
        } catch (DocServiceException e) {
            logger.error("Cannot send data", e);
        }
    }

    public void setDocService(DocServiceClient docServiceClient) {
        this.docServiceClient = docServiceClient;
    }

    /** Sends an ID-based message to the message queue. */
    public void sendIdMessage(Map<String, String> metadata, byte[] messageBytes) {
        metadata.put("content", "false");
        sendMessage(metadata, messageBytes);
    }

    /** Saves content to the document store, getting back the ID. */
    private String saveToDocumentStore(byte[] rawContent, String contentType) throws DocServiceException {
        if (docServiceClient == null) {
            logger.error("Cannot send data: document-service client has not been set");
            return "";
        }
        String docId = docServiceClient.store(rawContent, contentType);

        return docId;
    }

    /** Sends a raw-content message to the message queue. */
    public void sendContentMessage(Map<String, String> metadata, byte[] messageBytes) {
        metadata.put("content", "true");
        sendMessage(metadata, messageBytes);        
    }

    /** Sets up message queue and sends a message. */
    private void sendMessage(Map<String, String> metadata, byte[] messageBytes) {
        try {
            // Build AMPQ Basic Properties
            AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put("HasContent", metadata.get("content"));

            builder.contentType(metadata.get("contentType"));
            builder.deliveryMode(2 /* persistent */);
            builder.headers(headers);
            builder.timestamp(new java.util.Date());

            String dataType = metadata.get("dataType");
            String routingKey = "stucco.align." + dataType;

            // Send the file as a message
            channel.basicPublish(exchangeName, routingKey, builder.build(), messageBytes);

            // write to log the first N bytes of the message
            int messSize = Math.min(messageBytes.length, 100); 
            String messageSubString = new String(messageBytes, 0, messSize, "UTF-8");
            if (messageBytes.length > 100) {
                messageSubString += "...";
            }
            logger.info("Sent message for: {}, {}",dataType, messageSubString);

            logger.debug("RABBITMQ -> exchangeName: "+exchangeName+
                    "  dataType: "+dataType+
                    "  routingKey: "+ routingKey);

        } catch (IOException e) {
            logger.error("Error sending data though message queue", e);
        }
    }
}
