package gov.ornl.stucco.workers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.stanford.nlp.pipeline.Annotation;
import gov.ornl.stucco.ConfigLoader;
import gov.ornl.stucco.RabbitMQConsumer; 
import gov.ornl.stucco.RabbitMQMessage;
import gov.ornl.stucco.RabbitMQProducer;
import gov.ornl.stucco.RelationExtractor;
import gov.ornl.stucco.entity.EntityLabeler;
import gov.pnnl.stucco.doc_service_client.DocServiceClient;
import gov.pnnl.stucco.doc_service_client.DocServiceException;
import gov.ornl.stucco.alignment.PreprocessSTIX;
import gov.ornl.stucco.alignment.GraphConstructor;
import STIXExtractor.StuccoExtractor;

import org.mitre.stix.stix_1.STIXPackage;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 
import org.jdom2.Element;

public class UnstructuredWorker {
	private static final Logger logger = LoggerFactory.getLogger(UnstructuredWorker.class);
	private static final String PROCESS_NAME = "UNSTRUCTURED";
	
	private RabbitMQConsumer consumer;
	private RabbitMQProducer producer;
	
	private DocServiceClient docClient;
	private EntityLabeler entityLabeler;
	private RelationExtractor relationExtractor;
	private PreprocessSTIX preprocessSTIX;
	private GraphConstructor constructGraph;
	
	private boolean persistent;
	private int sleepTime;
	
	public UnstructuredWorker() {
		logger.info("loading config file from default location");
		ConfigLoader configLoader = new ConfigLoader();
		init(configLoader);
	}
	
	public UnstructuredWorker(String configFile) {
		logger.info("loading config file at: " + configFile);
		ConfigLoader configLoader = new ConfigLoader(configFile);
		init(configLoader);
	}
	
	private void init(ConfigLoader configLoader) {
		try {
			Map<String, Object> configMap;
			String exchange = null;
			String queue = null;
			String host = null;
			int port = -1;
			String user = null;
			String password = null;
			String[] bindingKeys = null;

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
			List<String> bindings = (List<String>)(configMap.get("bindings"));
			bindingKeys = new String[bindings.size()];
			bindingKeys = bindings.toArray(bindingKeys);

			logger.info("Connecting to rabbitMQ with this info: \nhost: " + host + "\nport: " + port + 
					"\nexchange: " + exchange + "\nqueue: " + queue + 
					"\nuser: " + user + "\npass: " + password);
			consumer = new RabbitMQConsumer(exchange, queue, host, port, user, password, bindingKeys);
			consumer.openQueue();

		} catch (FileNotFoundException e1) {
			logger.error("Error loading configuration.", e1);
			System.exit(-1);
		}
		catch (IOException e) {
			logger.error("Error initializing RabbitMQ connection.", e);
			System.exit(-1);
		}
		catch (Exception e) {
			logger.error("Error parsing configuration.", e);
			System.exit(-1);
		}
		logger.info("RabbitMQ (unstructured_data) connected.");

		try {
			Map<String, Object> configMap;
			String exchange = null;
			String queue = null;
			String host = null;
			int port = -1;
			String user = null;
			String password = null;
			String[] bindingKeys = null;

			configMap = configLoader.getConfig("alignment_data");
			exchange = String.valueOf(configMap.get("exchange"));
			queue = String.valueOf(configMap.get("queue"));
			host = String.valueOf(configMap.get("host"));
			port = Integer.parseInt(String.valueOf(configMap.get("port")));
			user = String.valueOf(configMap.get("username"));
			password = String.valueOf(configMap.get("password"));
			persistent = Boolean.parseBoolean(String.valueOf(configMap.get("persistent")));
			sleepTime = Integer.parseInt(String.valueOf(configMap.get("emptyQueueSleepTime")));
			@SuppressWarnings("unchecked")
			List<String> bindings = (List<String>)(configMap.get("bindings"));
			bindingKeys = new String[bindings.size()];
			bindingKeys = bindings.toArray(bindingKeys);

			logger.info("Connecting to rabbitMQ with this info: \nhost: " + host + "\nport: " + port + 
					"\nexchange: " + exchange + "\nqueue: " + queue + 
					"\nuser: " + user + "\npass: " + password);
			producer = new RabbitMQProducer(exchange, queue, host, port, user, password, bindingKeys);
			producer.openQueue();

		} catch (FileNotFoundException e1) {
			logger.error("Error loading configuration.", e1);
			System.exit(-1);
		}
		catch (IOException e) {
			logger.error("Error initializing RabbitMQ connection.", e);
			System.exit(-1);
		}
		catch (Exception e) {
			logger.error("Error parsing configuration.", e);
			System.exit(-1);
		}
		logger.info("RabbitMQ (alignment_data) connected.");

		try {
			preprocessSTIX = new PreprocessSTIX();
			constructGraph = new GraphConstructor();

			logger.info("preprocessSTIX and constructGraph created.  Connecting to document service...");

			Map<String, Object> configMap;
			String host = null;
			int port = -1;

			configMap = configLoader.getConfig("document_service");

			host = String.valueOf(configMap.get("host"));
			port = Integer.parseInt(String.valueOf(configMap.get("port")));
			docClient = new DocServiceClient(host, port);
		} catch (IOException e) {
			logger.error("Error initializing Document Service, Alignment and/or DB connection.", e);
			System.exit(-1);
		}
		logger.info("Alignment objects and Document service client created.  Initialization complete!");
	}
	
	public void run() {
		RabbitMQMessage message = null;
		boolean fatalError = false; //TODO only RMQ errors handled this way currently
		
		do{
			//Get message from the queue
			try{
				message = consumer.getMessage();
			} catch (IOException e) {
				logger.error("Encountered RabbitMQ IO error:", e);
				fatalError = true;
			}
			while (message != null && !fatalError) {
				long itemStartTime = System.currentTimeMillis();
				String routingKey = message.getRoutingKey().toLowerCase();
				long messageID = message.getId();
				
				if (message.getBody() != null) {
					String messageBody = new String(message.getBody());
					
					/*long timestamp = 0;
					if (response.getProps().getTimestamp() != null) {
						timestamp = response.getProps().getTimestamp().getTime();
					}*/
					
					boolean contentIncluded = false;
					Map<String, Object> headerMap = message.getHeaders();
					if ((headerMap != null) && (headerMap.containsKey("HasContent"))) {
						contentIncluded = Boolean.valueOf(String.valueOf(headerMap.get("HasContent")));
					}
					
					logger.debug("Recieved: " + routingKey + " deliveryTag=[" + messageID + "] message- "+ messageBody);
				
					//Get the document and title from the document server, if necessary
					String content = messageBody;
					String title = "";
					if (!contentIncluded) {
						String docId = content.trim();
						logger.debug("Retrieving document content from Document-Service for id '" + docId + "'.");
	
						try {
							JSONObject jsonObject = docClient.fetchExtractedText(docId);
							content = jsonObject.getString("document");
							try {
								title = jsonObject.getString("title");
							} catch (Exception ex) {
								title = docId;
							}
						} catch (DocServiceException e) {
							logger.error("Could not fetch document '" + docId + "' from Document-Service.", e);
							logger.error("Message content was:\n"+messageBody);
							logger.error("Skipping message: " + routingKey + " deliveryTag=[" + messageID + "]");
						} catch (Exception e) {
							logger.error("Other error in handling document '" + docId + "' from Document-Service.", e);
							logger.error("Message content was:\n"+messageBody);
						}
					}
					
					if (content != null) {
						try {
							//Label the entities/concepts in the document
							Annotation annotatedDoc = entityLabeler.getAnnotatedDoc(title, content);
						
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
							String graphString = relationExtractor.createSubgraph(annotatedDoc, dataSource);
							if (graphString != null) {
								try {
									JSONObject graph = new JSONObject(graphString);
									StuccoExtractor stuccoExt = new StuccoExtractor(graph);
									STIXPackage stixPackage = stuccoExt.getStixPackage();
									Map<String, Element> stixElements = preprocessSTIX.normalizeSTIX(stixPackage.toXMLString());
									graph = constructGraph.constructGraph(stixElements);
									sendToAlignment(graph);
								} catch (RuntimeException e) {
									logger.error("Error occurred with routingKey = " + routingKey);
									logger.error("										content = " + messageBody);
									logger.error("										source = " + dataSource);
									e.printStackTrace();
								}
							}
						} catch (Exception ex) {
							logger.error("Error occurred while extracting entities & relationships from the document '" + title + "'.");
							ex.printStackTrace();
						}
					}
					
					//Ack the message was processed and can be discarded from the queue
					try{
						logger.debug("Acking: " + routingKey + " deliveryTag=[" + messageID + "]");
						consumer.messageProcessed(messageID);
					} catch (IOException e) {
						logger.error("Encountered RabbitMQ IO error:", e);
						fatalError = true;
					}
				}
				else {
					try{
						consumer.retryMessage(messageID);
						logger.debug("Retrying: " + routingKey + " deliveryTag=[" + messageID + "]");
					} catch (IOException e) {
						logger.error("Encountered RabbitMQ IO error:", e);
						fatalError = true;
					}
				}
				
				//Get next message from queue
				try{
					message = consumer.getMessage();
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
			producer.close();
		} catch (IOException e) {
			logger.error("Encountered RabbitMQ IO error when closing connection:", e);
			//don't care in this case, exiting anyway.
		}
	}

	private void sendToAlignment(JSONObject graph) {
		Map<String, String> metadata = new HashMap<String,String>();
		metadata.put("contentType", "application/json");
		metadata.put("dataType", "graph");
		byte[] messageBytes = graph.toString().getBytes();
		producer.sendContentMessage(metadata, messageBytes);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		UnstructuredWorker unstructProcess;
		if (args.length == 0) {
			unstructProcess = new UnstructuredWorker();
		}
		else {
			unstructProcess = new UnstructuredWorker(args[0]);
		}
		unstructProcess.run();
	}
}
