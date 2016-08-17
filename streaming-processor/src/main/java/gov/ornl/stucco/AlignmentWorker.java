package gov.ornl.stucco;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.ornl.stucco.ConfigLoader;
import gov.ornl.stucco.RabbitMQConsumer;
import gov.ornl.stucco.RabbitMQMessage;
import gov.ornl.stucco.RabbitMQProducer;

import gov.pnnl.stucco.doc_service_client.DocServiceClient;
import gov.pnnl.stucco.doc_service_client.DocServiceException;
import gov.pnnl.stucco.doc_service_client.DocumentObject;

import gov.ornl.stucco.alignment.PreprocessSTIX;
import gov.ornl.stucco.alignment.GraphConstructor;
import gov.ornl.stucco.alignment.Align;

import STIXExtractor.ArgusExtractor;
import STIXExtractor.BugtraqExtractor;
import STIXExtractor.CaidaExtractor;
import STIXExtractor.CIF1d4Extractor;
import STIXExtractor.CIFZeusTrackerExtractor;
import STIXExtractor.CIFEmergingThreatsExtractor;
import STIXExtractor.CleanMxVirusExtractor;
import STIXExtractor.ClientBannerExtractor;
import STIXExtractor.CpeExtractor;
import STIXExtractor.CveExtractor;
import STIXExtractor.DNSRecordExtractor;
import STIXExtractor.FSecureExtractor;
import STIXExtractor.GeoIPExtractor;
import STIXExtractor.HoneExtractor;
import STIXExtractor.HTTPDataExtractor;
import STIXExtractor.LoginEventExtractor;
import STIXExtractor.MalwareDomainListExtractor;
import STIXExtractor.MetasploitExtractor;
import STIXExtractor.NvdToStixExtractor;
import STIXExtractor.PackageListExtractor;
import STIXExtractor.ServerBannerExtractor;
import STIXExtractor.ServiceListExtractor;
import STIXExtractor.SophosExtractor;

import org.mitre.stix.stix_1.STIXPackage;
import org.mitre.cybox.cybox_2.Observables;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jdom2.Element;

public class AlignmentWorker {
	private static final Logger logger = LoggerFactory.getLogger(AlignmentWorker.class);
	private static final String PROCESS_NAME = "ALIGNMENT_WORKER";

	private RabbitMQConsumer consumer;

	private Align alignment;
	
	private boolean persistent;
	private int sleepTime;
	
	public AlignmentWorker() {
		logger.info("loading config file from default location");
		ConfigLoader configLoader = new ConfigLoader();
		init(configLoader);
	}
	
	public AlignmentWorker(String configFile) {
		logger.info("loading config file at: " + configFile);
		ConfigLoader configLoader = new ConfigLoader(configFile);
		init(configLoader);
	}
	
	private void init(ConfigLoader configLoader) {
		Map<String, Object> configMap;
		String exchange = null;
		String queue = null;
		String host = null;
		int port = -1;
		String user = null;
		String password = null;
		String[] bindingKeys = null;
		try {
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
		} catch (FileNotFoundException e1) {
			logger.error("Error loading configuration.", e1);
			System.exit(-1);
		} catch (Exception e) {
			logger.error("Error parsing configuration.", e);
			System.exit(-1);
		}
		logger.info("Config file loaded and parsed");
		
		try {
			logger.info("Connecting to rabbitMQ with this info: \nhost: " + host + "\nport: " + port + 
					"\nexchange: " + exchange + "\nqueue: " + queue + 
					"\nuser: " + user + "\npass: " + password);
			consumer = new RabbitMQConsumer(exchange, queue, host, port, user, password, bindingKeys);
			consumer.openQueue();
		} catch (IOException e) {
			logger.error("Error initializing RabbitMQ connection.", e);
			System.exit(-1);
		}
		logger.info("RabbitMQ connected.");
		try {
			alignment = new Align();
			
			logger.info("DB connection created.");
		} catch (Exception e) {
			logger.error("Error initializing Alignment and/or DB connection.", e);
			System.exit(-1);
		}
		logger.info("Alignment obj, DB connection, and Document service client created.  Initialization complete!");
	}

	public void run() {
		RabbitMQMessage message = null;
		boolean fatalError = false; //TODO only RMQ errors handled this way currently
		
		do {
			//Get message from the queue
			try {
				message = consumer.getMessage();
			} catch (IOException e) {
				logger.error("Encountered RabbitMQ IO error:", e);
				fatalError = true;
			}
			while (message != null && !fatalError) {
				long itemStartTime = System.currentTimeMillis();
				String routingKey = message.getRoutingKey().toLowerCase();
				long messageID = message.getId();
				
				String messageBody = message.getBody();
				if (messageBody != null) {
					
					/*long timestamp = 0;
					if (response.getProps().getTimestamp() != null) {
						timestamp = response.getProps().getTimestamp().getTime();
					}*/
	
					boolean contentIncluded = false;
					Map<String, Object> headerMap = message.getHeaders();
					if ((headerMap != null) && (headerMap.containsKey("HasContent"))) {
						contentIncluded = Boolean.valueOf(String.valueOf(headerMap.get("HasContent")));
					}
					
					if (!contentIncluded){
						logger.warn("Message `HasContent` flag not set to `true`, which is required!");
					}
					
					logger.debug("Recieved: " + routingKey + " deliveryTag=[" + messageID + "] message- "+ messageBody);
				
					//pass the graph to alignment to laod
					JSONObject graph = new JSONObject(messageBody);
					if (graph != null) {
						alignment.load(graph);
					}
					
					//Ack the message was processed and can be discarded from the queue
					try {
						logger.debug("Acking: " + routingKey + " deliveryTag=[" + messageID + "]");
						consumer.messageProcessed(messageID);
					} catch (IOException e) {
						logger.error("Encountered RabbitMQ IO error:", e);
						fatalError = true;
					}
				}
				else {
					try {
						consumer.retryMessage(messageID);
						logger.debug("Retrying: " + routingKey + " deliveryTag=[" + messageID + "]");
					} catch (IOException e) {
						logger.error("Encountered RabbitMQ IO error:", e);
						fatalError = true;
					}
				}
				
				long itemEndTime = System.currentTimeMillis();
				logger.debug( "Finished processing item in " + (itemEndTime - itemStartTime) + " ms. " +
						" routingKey: " + routingKey + " deliveryTag: " + messageID + " message: " + messageBody);

				//Get next message from queue
				try {
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
		} catch (IOException e) {
			logger.error("Encountered RabbitMQ IO error when closing connection:", e);
			//don't care in this case, exiting anyway.
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AlignmentWorker structProcess;
		if (args.length == 0) {
			structProcess = new AlignmentWorker();
		}
		else {
			structProcess = new AlignmentWorker(args[0]);
		}
		structProcess.run();
	}
}
