package gov.ornl.stucco.structured;

import java.util.List;
import java.util.Map;

import gov.ornl.stucco.ConfigLoader;
import gov.ornl.stucco.RabbitMQConsumer;
import gov.ornl.stucco.extractors.ArgusExtractor;
import gov.ornl.stucco.extractors.CleanMxVirusExtractor;
import gov.ornl.stucco.extractors.CpeExtractor;
import gov.ornl.stucco.extractors.CveExtractor;
import gov.ornl.stucco.extractors.GeoIPExtractor;
import gov.ornl.stucco.extractors.HoneExtractor;
import gov.ornl.stucco.extractors.LoginEventExtractor;
import gov.ornl.stucco.extractors.MetasploitExtractor;
import gov.ornl.stucco.extractors.NvdExtractor;
import gov.ornl.stucco.extractors.PackageListExtractor;
import gov.ornl.stucco.morph.ast.ValueNode;
import gov.ornl.stucco.morph.parser.CsvParser;
import gov.ornl.stucco.morph.parser.ParsingException;
import gov.ornl.stucco.morph.parser.XmlParser;
import gov.pnnl.stucco.doc_service_client.DocServiceClient;
import gov.pnnl.stucco.doc_service_client.DocServiceException;
import gov.pnnl.stucco.doc_service_client.DocumentObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alignment.alignment_v2.Align;
import HTMLExtractor.FSecureExtractor;
import HTMLExtractor.MalwareDomainListExtractor;
import HTMLExtractor.SophosExtractor;
import HTMLExtractor.BugtraqExtractor;

import com.rabbitmq.client.GetResponse;

public class StructuredTransformer {
	private static final Logger logger = LoggerFactory.getLogger(StructuredTransformer.class);
	private static final String PROCESS_NAME = "STRUCTURED";

	private RabbitMQConsumer consumer;

	private DocServiceClient docClient;
	private Align alignment;
	
	private boolean persistent;
	private int sleepTime;
	
	public StructuredTransformer() {
		ConfigLoader configLoader = new ConfigLoader();
		init(configLoader);
	}
	
	public StructuredTransformer(String configFile) {
		ConfigLoader configLoader = new ConfigLoader(configFile);
		init(configLoader);
	}
	
	private void init(ConfigLoader configLoader) {
		Map<String, Object> configMap = configLoader.getConfig("structured_data");
		String exchange = String.valueOf(configMap.get("exchange"));
		String queue = String.valueOf(configMap.get("queue"));
		String host = String.valueOf(configMap.get("host"));
		int port = Integer.parseInt(String.valueOf(configMap.get("port")));
		String user = String.valueOf(configMap.get("username"));
		String password = String.valueOf(configMap.get("password"));
		persistent = Boolean.parseBoolean(String.valueOf(configMap.get("persistent")));
		sleepTime = Integer.parseInt(String.valueOf(configMap.get("emptyQueueSleepTime")));
		@SuppressWarnings("unchecked")
		List<String> bindings = (List<String>) configMap.get("bindings");
		String[] bindingKeys = new String[bindings.size()];
		bindingKeys = bindings.toArray(bindingKeys);
		consumer = new RabbitMQConsumer(exchange, queue, host, port, user, password, bindingKeys);
		consumer.openQueue();
		
		alignment = new Align();
		
		configMap = configLoader.getConfig("document_service");
		host = String.valueOf(configMap.get("host"));
		port = Integer.parseInt(String.valueOf(configMap.get("port")));
		docClient = new DocServiceClient(host, port);
	}
	
	
	public void run() {
		GetResponse response;
		
		do{
			//Get message from the queue
			response = consumer.getMessage();
			while (response != null) {
				String routingKey = response.getEnvelope().getRoutingKey().toLowerCase();
				long deliveryTag = response.getEnvelope().getDeliveryTag();
				
				if (response.getBody() != null) {
					String message = new String(response.getBody());
					
					long timestamp = 0;
					if (response.getProps().getTimestamp() != null) {
						timestamp = response.getProps().getTimestamp().getTime();
					}
	
					boolean contentIncluded = false;
					Map<String, Object> headerMap = response.getProps().getHeaders();
					if ((headerMap != null) && (headerMap.containsKey("HasContent"))) {
						contentIncluded = Boolean.valueOf(String.valueOf(headerMap.get("HasContent")));
					}
					
					logger.debug("Recieved: " + routingKey + " deliveryTag=[" + deliveryTag + "] message- "+ message);
				
					//Get the document from the document server, if necessary
					String content = message;
					if (!contentIncluded && !routingKey.contains(".sophos") && !routingKey.contains(".bugtraq")) {
						String docId = content.trim();
						logger.debug("Retrieving document content from Document-Service for id '" + docId + "'.");
	
						try {
							DocumentObject document = docClient.fetch(docId);
							content = document.getDataAsString();
						} catch (DocServiceException e) {
							logger.error("Could not fetch document '" + docId + "' from Document-Service.", e);
							logger.error("Message content was:\n"+message);
						}
					}
					
					//Construct the subgraph by parsing the structured data
					String graph = null;
					
					if (routingKey.contains(".cve")) {
						ValueNode parsedData = null;
						try{
							ValueNode nodeData = XmlParser.apply(content);
							parsedData = (ValueNode) CveExtractor.extract(nodeData);
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing cve!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing cve!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
						if(parsedData != null){
							graph = String.valueOf(parsedData);
						}
					}
					else if (routingKey.contains(".nvd")) {
						ValueNode parsedData = null;
						try{
							ValueNode nodeData = XmlParser.apply(content);
							parsedData = (ValueNode) NvdExtractor.extract(nodeData);
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing nvd!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing nvd!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
						if(parsedData != null){
							graph = String.valueOf(parsedData);
						}
					}
					else if (routingKey.contains(".cpe")) {
						ValueNode parsedData = null;
						try{
							ValueNode nodeData = XmlParser.apply(content);
							parsedData = (ValueNode) CpeExtractor.extract(nodeData);
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing cpe!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing cpe!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
						if(parsedData != null){
							graph = String.valueOf(parsedData);
						}
					}
					else if (routingKey.contains(".maxmind")) {
						ValueNode parsedData = null;
						try{
							ValueNode nodeData = CsvParser.apply(content);
							parsedData = (ValueNode) GeoIPExtractor.extract(nodeData);
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing maxmind!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing maxmind!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
						if(parsedData != null){
							graph = String.valueOf(parsedData);
						}
					}
					else if (routingKey.contains(".argus")) {
						ValueNode parsedData = null;
						try{
							ValueNode nodeData = CsvParser.apply(content);
							parsedData = (ValueNode) ArgusExtractor.extract(nodeData);
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing argus!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing argus!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
						if(parsedData != null){
							graph = String.valueOf(parsedData);
						}
					}
					else if (routingKey.contains(".hone")) {
						ValueNode parsedData = null;
						try{
							ValueNode nodeData = CsvParser.apply(content);
							parsedData = (ValueNode) HoneExtractor.extract(nodeData);
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing hone!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing hone!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
						if(parsedData != null){
							graph = String.valueOf(parsedData);
						}
					}
					else if (routingKey.contains(".metasploit")) {
						ValueNode parsedData = null;
						try{
							ValueNode nodeData = CsvParser.apply(content);
							parsedData = (ValueNode) MetasploitExtractor.extract(nodeData);
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing metasploit!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing metasploit!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
						if(parsedData != null){
							graph = String.valueOf(parsedData);
						}
					}
					else if (routingKey.replaceAll("\\-", "").contains(".cleanmx")) {
						ValueNode parsedData = null;
						try{
							ValueNode nodeData = XmlParser.apply(content);
							parsedData = (ValueNode) CleanMxVirusExtractor.extract(nodeData);
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing cleanmx!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing cleanmx!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
						if(parsedData != null){
							graph = String.valueOf(parsedData);
						}
					}else if (routingKey.contains(".sophos")) {//TODO: testing
						try{
							String summary = null;
							String details = null;
							String[] items = content.split("\\r?\\n");
							for(String item : items){
								String docId = item.split("\\s+")[0];
								String sourceURL = item.split("\\s+")[1];
								String itemContent = null;
								try {
									DocumentObject document = docClient.fetch(docId);
									itemContent = document.getDataAsString();
								} catch (DocServiceException e) {
									logger.error("Could not fetch document '" + docId + "' from Document-Service. URL was: " + sourceURL, e);
									logger.error("Complete message content was:\n"+content);
								}
								if(sourceURL.contains("/detailed-analysis.aspx")){
									details = itemContent;
								}else if(sourceURL.contains(".aspx")){
									summary = itemContent;
								}else{
									logger.warn("unexpected URL (sophos) " + sourceURL);
								}
							}
							if(summary != null && details != null){
								SophosExtractor sophosExt = new SophosExtractor(summary, details);
								graph = sophosExt.getGraph().toString();
							}else{
								logger.warn("Sophos: some required fields were null, skipping group.\nMessage was:" + content);
							}
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing sophos!", e);
							if (!contentIncluded) logger.error("Problem docid was one of:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing sophos!", e);
							if (!contentIncluded) logger.error("Problem docid was one of:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
					}else if (routingKey.replaceAll("\\-", "").contains(".fsecure")) {//TODO: testing
						try {
								FSecureExtractor fSecureExt = new FSecureExtractor(content);
								graph = fSecureExt.getGraph().toString();
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing fsecure!", e);
							if (!contentIncluded) logger.error("Problem message was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing fsecure!", e);
							if (!contentIncluded) logger.error("Problem message was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
					}else if (routingKey.contains(".malwaredomainlist")) {//TODO: testing
						try {
								MalwareDomainListExtractor mdlExt = new MalwareDomainListExtractor(content);
								graph = mdlExt.getGraph().toString();
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing malwaredomainlist!", e);
							if (!contentIncluded) logger.error("Problem message was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing malwaredomainlist!", e);
							if (!contentIncluded) logger.error("Problem message was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
					}else if (routingKey.contains(".bugtraq")) {//TODO: testing
						try{
							String info = null;
							String discussion = null;
							String exploit = null;
							String solution = null;
							String references = null;
							String[] items = content.split("\\r?\\n");
							for(String item : items){
								String docId = item.split("\\s+")[0];
								String sourceURL = item.split("\\s+")[1];
								String itemContent = null;
								try {
									DocumentObject document = docClient.fetch(docId);
									itemContent = document.getDataAsString();
								} catch (DocServiceException e) {
									logger.error("Could not fetch document '" + docId + "' from Document-Service. URL was: " + sourceURL, e);
									logger.error("Complete message content was:\n"+content);
								}
								if(sourceURL.contains("/info")){
									info = itemContent;
								}else if(sourceURL.contains("/discuss")){ //interestingly, "/discuss" and "/discussion" are both valid urls for this item
									discussion = itemContent;
								}else if(sourceURL.contains("/exploit")){
									exploit = itemContent;
								}else if(sourceURL.contains("/solution")){
									solution = itemContent;
								}else if(sourceURL.contains("/references")){
									references = itemContent;
								}else{
									logger.warn("unexpected URL (bugtraq) " + sourceURL);
								}
							}
							if(info != null && discussion != null && exploit != null && solution != null && references != null){
								BugtraqExtractor bugtraqExt = new BugtraqExtractor(info, discussion, exploit, solution, references);
								graph = bugtraqExt.getGraph().toString();
							}else{
								logger.warn("Bugtraq: some required fields were null, skipping group.\nMessage was:" + content);
							}
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing bugtraq!", e);
							if (!contentIncluded) logger.error("Problem docid was one of:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing bugtraq!", e);
							if (!contentIncluded) logger.error("Problem docid was one of:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
					}
					else if (routingKey.contains(".login_events")) {
						ValueNode parsedData = null;
						try{
							ValueNode nodeData = CsvParser.apply(content);
							parsedData = (ValueNode) LoginEventExtractor.extract(nodeData);
						} catch (ParsingException e) {
							logger.error("ParsingException in parsing login events!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing login events!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
						if(parsedData != null){
							graph = String.valueOf(parsedData);
						}
					}
					else if (routingKey.contains(".installed_package")) {
						ValueNode parsedData = null;
						try{
							ValueNode nodeData = CsvParser.apply(content);
							parsedData = (ValueNode) PackageListExtractor.extract(nodeData);
						} catch (ParsingException e) {
							logger.error("ParsingException in package list!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						} catch (Exception e) {
							logger.error("Other Error in parsing package list!", e);
							if (!contentIncluded) logger.error("Problem docid was:\n"+message);
							else logger.error("Problem content was:\n"+content);
							graph = null;
						}
						if(parsedData != null){
							graph = String.valueOf(parsedData);
						}
					}
					else {
						logger.warn("Unexpected routing key encountered '" + routingKey + "'.");
					}
	
					//TODO: Add timestamp into subgraph
					//Merge subgraph into full knowledge graph
					if(graph != null) alignment.load(graph);
					
					//Ack the message was processed and can be discarded from the queue
					logger.debug("Acking: " + routingKey + " deliveryTag=[" + deliveryTag + "]");
					consumer.messageProcessed(deliveryTag);
				}
				else {
					consumer.retryMessage(deliveryTag);
					logger.debug("Retrying: " + routingKey + " deliveryTag=[" + deliveryTag + "]");
				}
				
				//Get next message from queue
				response = consumer.getMessage();
			}
			try{
				Thread.sleep(sleepTime);
			} catch (InterruptedException consumed) {
				//don't care in this case, exiting anyway.
			}
		}while(persistent);
		consumer.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		StructuredTransformer structProcess;
		if(args.length == 0){
			structProcess = new StructuredTransformer();
		}
		else{
			structProcess = new StructuredTransformer(args[0]);
		}
		structProcess.run();
	}

}
