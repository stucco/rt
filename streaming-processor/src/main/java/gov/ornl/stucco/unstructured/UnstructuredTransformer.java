package gov.ornl.stucco.unstructured;

import edu.stanford.nlp.pipeline.Annotation;
import gov.ornl.stucco.ConfigLoader;
import gov.ornl.stucco.RabbitMQConsumer;
import gov.ornl.stucco.entity.EntityLabeler;
import gov.pnnl.stucco.doc_service_client.DocServiceClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.GetResponse;

public class UnstructuredTransformer {
	private static final Logger logger = LoggerFactory.getLogger(UnstructuredTransformer.class);
	private static final String PROCESS_NAME = "UNSTRUCTURED";
	
	private RabbitMQConsumer consumer;
	private DocServiceClient docClient;
	private EntityLabeler entityLabeler;
	private File documentDir;
//	private RelationExtractor relationExtractor;
//	private PreprocessSTIX preprocessSTIX;
//	private GraphConstructor constructGraph;
//	private Align alignment;
	
	private boolean persistent;
	private int sleepTime;
	
	public UnstructuredTransformer() {
		logger.info("loading config file from default location");
		ConfigLoader configLoader = new ConfigLoader();
		init(configLoader);
	}
	
	public UnstructuredTransformer(String configFile) {
		logger.info("loading config file at: " + configFile);
		ConfigLoader configLoader = new ConfigLoader(configFile);
		init(configLoader);
	}
	
	private void init(ConfigLoader configLoader){
		Map<String, Object> configMap;
		String exchange = null;
		String queue = null;
		String host = null;
		int port = -1;
		String user = null;
		String password = null;
		String[] bindingKeys = null;
		String docDir = "../documents/";
		try {
			configMap = configLoader.getConfig("unstructured_data");
			exchange = String.valueOf(configMap.get("exchange"));
			queue = String.valueOf(configMap.get("queue"));
			host = String.valueOf(configMap.get("host"));
			port = Integer.parseInt(String.valueOf(configMap.get("port")));
			user = String.valueOf(configMap.get("username"));
			password = String.valueOf(configMap.get("password"));
			persistent = Boolean.parseBoolean(String.valueOf(configMap.get("persistent")));
			sleepTime = Integer.parseInt(String.valueOf(configMap.get("emptyQueueSleepTime")));
			@SuppressWarnings("unchecked")
			List<String> bindings = (List<String>) configMap.get("bindings");
			bindingKeys = new String[bindings.size()];
			bindingKeys = bindings.toArray(bindingKeys);
			docDir = String.valueOf(configMap.get("serialObjectDir"));
		} catch (FileNotFoundException e1) {
			logger.error("Error loading configuration.", e1);
			System.exit(-1);
		} catch (Exception e) {
			logger.error("Error parsing configuration.", e);
			System.exit(-2);
		}
		logger.info("Config file loaded and parsed");
		
		try {
			logger.info("Connecting to rabbitMQ with this info: \nhost: " + host + "\nport: " + port + 
					"\nexchange: " + exchange + "\nqueue: " + queue + 
					"\nuser: " + user + "\npass: " + password);
			consumer = new RabbitMQConsumer(exchange, queue, host, port, user, password, bindingKeys);
			consumer.openQueue();
			
			entityLabeler = new EntityLabeler();
			
//			relationExtractor = new RelationExtractor();
//			
//			preprocessSTIX = new PreprocessSTIX();
//			constructGraph = new GraphConstructor();
//			alignment = new Align();
			
			configMap = configLoader.getConfig("document_service");
			
			host = String.valueOf(configMap.get("host"));
			port = Integer.parseInt(String.valueOf(configMap.get("port")));
			docClient = new DocServiceClient(host, port);
			
			documentDir = new File(docDir);
			if (!documentDir.exists()) {
				documentDir.mkdir();
			}
		} catch (IOException e) {
			logger.error("Error initializing Alignment and/or DB connection.", e);
			System.exit(-4);
		}
		logger.info("Alignment obj, DB connection, and Document service client created.  Initialization complete!");
	}

	
	public void run() {
		GetResponse response = null;
		boolean fatalError = false; //TODO only RMQ errors handled this way currently
		
		do{
			//Get message from the queue
			try{
				response = consumer.getMessage();
			} catch (IOException e) {
				logger.error("Encountered RabbitMQ IO error:", e);
				fatalError = true;
			}
			while (response != null && !fatalError) {
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
				
					//Get the document and title from the document server, if necessary
					String content = message;
					String title = "";
					if (!contentIncluded) {
						String docId = content.trim();
						logger.debug("Retrieving document content from Document-Service for id '" + docId + "'.");
	
						try {
							JSONObject jsonObject = docClient.fetchExtractedText(docId);
							content = jsonObject.getString("document");
							title = jsonObject.getString("title");
						} catch (Exception e) {
							logger.error("Could not fetch document '" + docId + "' from Document-Service.", e);
						}
					}
					
					//Extract the data source name from the routing key
					String dataSource = routingKey;
					int index = routingKey.indexOf(PROCESS_NAME.toLowerCase());
					if (index > -1) {
						dataSource = routingKey.substring(index + PROCESS_NAME.length());
						if (dataSource.startsWith(".")) {
							dataSource = dataSource.substring(1);
						}
					}
					
					//Label the entities/concepts in the document
					Annotation annotatedDoc = entityLabeler.getAnnotatedDoc(title, content);
					EntityLabeler.serializeAnnotatedDoc(annotatedDoc, title, dataSource, documentDir.getPath());
					
//					//Construct the subgraph from the concepts and relationships
//					String graphString = relationExtractor.createSubgraph(annotatedDoc, dataSource);
//					if (graphString != null) {
//						try {
//							JSONObject graph = new JSONObject(graphString);
//							StuccoExtractor stuccoExt = new StuccoExtractor(graph);
//							STIXPackage stixPackage = stuccoExt.getStixPackage();
//							Map<String, Element> stixElements = preprocessSTIX.normalizeSTIX(stixPackage.toXMLString());
//							graph = constructGraph.constructGraph(stixElements);
//							alignment.load(graph);
//						} catch (RuntimeException e) {
//							logger.error("Error occurred with routingKey = " + routingKey);
//							logger.error("										content = " + message);
//							logger.error("										source = " + dataSource);
//							e.printStackTrace();
//						}
//					}
					//TODO: Add timestamp into subgraph
					//Merge subgraph into full knowledge graph
				//	alignment.load(graph);
					
					//Ack the message was processed and can be discarded from the queue
					try{
						logger.debug("Acking: " + routingKey + " deliveryTag=[" + deliveryTag + "]");
						consumer.messageProcessed(deliveryTag);
					} catch (IOException e) {
						logger.error("Encountered RabbitMQ IO error:", e);
						fatalError = true;
					}
				}
				else {
					try{
						consumer.retryMessage(deliveryTag);
						logger.debug("Retrying: " + routingKey + " deliveryTag=[" + deliveryTag + "]");
					} catch (IOException e) {
						logger.error("Encountered RabbitMQ IO error:", e);
						fatalError = true;
					}
				}
				
				//Get next message from queue
				try{
					response = consumer.getMessage();
				} catch (IOException e) {
					logger.error("Encountered RabbitMQ IO error:", e);
					fatalError = true;
				}
			}
			
			//Either the queue is empty, or an error occurred.
			//Either way, sleep for a bit to prevent rapid loop of re-starting.
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException consumed) {
				//don't care in this case, exiting anyway.
			}
		} while (persistent && !fatalError);
		try {
			consumer.close();
		} catch (IOException e) {
			logger.error("Encountered RabbitMQ IO error when closing connection:", e);
			//don't care in this case, exiting anyway.
		}
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		UnstructuredTransformer unstructProcess;
		if (args.length == 0) {
			unstructProcess = new UnstructuredTransformer();
		}
		else {
			unstructProcess = new UnstructuredTransformer(args[0]);
		}
		unstructProcess.run();
	}
}
