package gov.ornl.stucco.unstructured;

import gov.ornl.stucco.RabbitMQConsumer;
import gov.ornl.stucco.RelationExtractor;
import gov.ornl.stucco.entity.EntityExtractor;
import gov.ornl.stucco.entity.models.Sentences;
import gov.pnnl.stucco.doc_service_client.DocServiceClient;
import gov.pnnl.stucco.doc_service_client.DocServiceException;
import gov.pnnl.stucco.doc_service_client.DocumentObject;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alignment.alignment_v2.Align;

import com.rabbitmq.client.GetResponse;

public class UnstructuredTransformer {
	private static final Logger logger = LoggerFactory.getLogger(UnstructuredTransformer.class);
	
	private static final String PROCESS_NAME = "UNSTRUCTURED";
	
	private final String[] bindingKeys = {"stucco.in.unstructured.#"};
	private RabbitMQConsumer consumer;
	
	private DocServiceClient docClient;
	private EntityExtractor entityExtractor;
	private RelationExtractor relationExtractor;
	private Align alignment;
	
	//TODO pull config from yaml file or etcd
	public UnstructuredTransformer(/*configurations*/) {
		consumer = new RabbitMQConsumer("stucco", "stucco-in-unstructured", "localhost", 5672, "guest", "guest", bindingKeys);
		consumer.openQueue();
		
		try {
			entityExtractor = new EntityExtractor();
		} catch (Exception e) {
			logger.error("Error loading EntityExtractor models.", e);
		}
		
		relationExtractor = new RelationExtractor();
		
		alignment = new Align();
		
		docClient = new DocServiceClient("localhost", 8118);
	}

	
	public void run() {
		//Get message from the queue
		GetResponse response = consumer.getMessage();
		
		while (response != null) {
			String routingKey = response.getEnvelope().getRoutingKey();
			long deliveryTag = response.getEnvelope().getDeliveryTag();
			
			if (response.getBody() != null) {
				String message = new String(response.getBody());
				
				long timestamp = response.getProps().getTimestamp().getTime();
				boolean contentIncluded = false;
				Map<String, Object> headerMap = response.getProps().getHeaders();
				if ((headerMap != null) && (headerMap.containsKey("HasContent"))) {
					contentIncluded = Boolean.valueOf(String.valueOf(headerMap.get("HasContent")));
				}
				
				logger.debug("Recieved: " + routingKey + " deliveryTag=[" + deliveryTag + "] message- "+ message);
			
				//Get the document from the document server, if necessary
				String content = message;
				if (!contentIncluded) {
					String docId = content.trim();
					logger.debug("Retrieving document content from Document-Service for id '" + docId + "'.");

					try {
						//TODO: get the extracted text here
						DocumentObject document = docClient.fetch(docId);
						content = document.getDataAsString();
					} catch (DocServiceException e) {
						logger.error("Could not fetch document '" + docId + "' from Document-Service.", e);
					}
				}
				
				//Label the entities/concepts in the document
				Sentences sentences = entityExtractor.getAnnotatedText(content);
				
				//Extract the data source name from the routing key
				String dataSource = routingKey;
				int index = routingKey.indexOf(PROCESS_NAME.toLowerCase());
				if (index > -1) {
					dataSource = routingKey.substring(index + PROCESS_NAME.length());
					if (dataSource.startsWith(".")) {
						dataSource = dataSource.substring(1);
					}
				}
				//Construct the subgraph from the concepts and relationships
				String graph = relationExtractor.getGraph(dataSource, sentences);
				
				//TODO: Add timestamp into subgraph
				//Merge subgraph into full knowledge graph
				alignment.load(graph);
				
				//Ack the message was processed and can be discarded from the queue
				logger.info("Acking: " + routingKey + " deliveryTag=[" + deliveryTag + "]");
				consumer.messageProcessed(deliveryTag);
			}
			else {
				consumer.retryMessage(deliveryTag);
				logger.info("Retrying: " + routingKey + " deliveryTag=[" + deliveryTag + "]");
			}
			
			//Get next message from queue
			response = consumer.getMessage();
		}
		
		consumer.close();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		UnstructuredTransformer unstructProcess = new UnstructuredTransformer();
		unstructProcess.run();
	}
}
