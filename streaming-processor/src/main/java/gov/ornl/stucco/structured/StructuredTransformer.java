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
import gov.ornl.stucco.extractors.MetasploitExtractor;
import gov.ornl.stucco.extractors.NvdExtractor;
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
import HTMLExtractor.SophosExtractor;
import HTMLExtractor.BugtraqExtractor;

import com.rabbitmq.client.GetResponse;

public class StructuredTransformer {
	private static final Logger logger = LoggerFactory.getLogger(StructuredTransformer.class);
	private static final String PROCESS_NAME = "STRUCTURED";

	private RabbitMQConsumer consumer;

	private DocServiceClient docClient;
	private Align alignment;
	private int sleepTime;
	
	public StructuredTransformer() {
		Map<String, Object> configMap = ConfigLoader.getConfig("structured_data");
		String exchange = String.valueOf(configMap.get("exchange"));
		String queue = String.valueOf(configMap.get("queue"));
		String host = String.valueOf(configMap.get("host"));
		int port = Integer.parseInt(String.valueOf(configMap.get("port")));
		String user = String.valueOf(configMap.get("username"));
		String password = String.valueOf(configMap.get("password"));
		int sleepTime = Integer.parseInt(String.valueOf(configMap.get("emptyQueueSleepTime")));
		@SuppressWarnings("unchecked")
		List<String> bindings = (List<String>) configMap.get("bindings");
		String[] bindingKeys = new String[bindings.size()];
		bindingKeys = bindings.toArray(bindingKeys);
		consumer = new RabbitMQConsumer(exchange, queue, host, port, user, password, bindingKeys);
		consumer.openQueue();
		
		alignment = new Align();
		
		configMap = ConfigLoader.getConfig("document_service");
		host = String.valueOf(configMap.get("host"));
		port = Integer.parseInt(String.valueOf(configMap.get("port")));
		docClient = new DocServiceClient(host, port);
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
						DocumentObject document = docClient.fetch(docId);
						content = document.getDataAsString();
					} catch (DocServiceException e) {
						logger.error("Could not fetch document '" + docId + "' from Document-Service.", e);
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
					} catch (Exception e) {
						logger.error("Other Error in parsing cve!", e);
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
					} catch (Exception e) {
						logger.error("Other Error in parsing nvd!", e);
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
					} catch (Exception e) {
						logger.error("Other Error in parsing cpe!", e);
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
					} catch (Exception e) {
						logger.error("Other Error in parsing maxmind!", e);
					}
					if(parsedData != null){
						graph = String.valueOf(parsedData);
					}
				}
				else if (routingKey.contains(".argus")) {
					ValueNode parsedData = null;
					try{
						ValueNode nodeData = XmlParser.apply(content);
						parsedData = (ValueNode) ArgusExtractor.extract(nodeData);
					} catch (ParsingException e) {
						logger.error("ParsingException in parsing argus!", e);
					} catch (Exception e) {
						logger.error("Other Error in parsing argus!", e);
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
					} catch (Exception e) {
						logger.error("Other Error in parsing hone!", e);
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
					} catch (Exception e) {
						logger.error("Other Error in parsing metasploit!", e);
					}
					if(parsedData != null){
						graph = String.valueOf(parsedData);
					}
				}
				else if (routingKey.contains(".cleanmx")) {
					ValueNode parsedData = null;
					try{
						ValueNode nodeData = XmlParser.apply(content);
						parsedData = (ValueNode) CleanMxVirusExtractor.extract(nodeData);
					} catch (ParsingException e) {
						logger.error("ParsingException in parsing cleanmx!", e);
					} catch (Exception e) {
						logger.error("Other Error in parsing cleanmx!", e);
					}
					if(parsedData != null){
						graph = String.valueOf(parsedData);
					}
				}else if (routingKey.contains(".sophos")) {//TODO: testing
					ValueNode parsedData = null;
					try{
						String summary = null;
						String details = null;
						String[] items = content.split("\n");
						for(String item : items){
							String docId = item.split(" ")[0];
							String sourceURL = item.split(" ")[1];
							String itemContent = null;
							try {
								DocumentObject document = docClient.fetch(docId);
								itemContent = document.getDataAsString();
							} catch (DocServiceException e) {
								logger.error("Could not fetch document '" + docId + "' from Document-Service. URL was: " + sourceURL, e);
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
					} catch (Exception e) {
						logger.error("Other Error in parsing sophos!", e);
					}
				}else if (routingKey.contains(".bugtraq")) {//TODO: testing
					ValueNode parsedData = null;
					try{
						String info = null;
						String discussion = null;
						String exploit = null;
						String solution = null;
						String references = null;
						String[] items = content.split("\n");
						for(String item : items){
							String docId = item.split(" ")[0];
							String sourceURL = item.split(" ")[1];
							String itemContent = null;
							try {
								DocumentObject document = docClient.fetch(docId);
								itemContent = document.getDataAsString();
							} catch (DocServiceException e) {
								logger.error("Could not fetch document '" + docId + "' from Document-Service. URL was: " + sourceURL, e);
							}
							if(sourceURL.contains("/info")){
								info = itemContent;
							}else if(sourceURL.contains("/discussion")){
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
					} catch (Exception e) {
						logger.error("Other Error in parsing bugtraq!", e);
					}
				}
				else {
					logger.warn("Unexpected routing key encountered '" + routingKey + "'.");
				}

				//TODO: Add timestamp into subgraph
				//Merge subgraph into full knowledge graph
				alignment.load(graph);
				
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
		consumer.close();
		try{
			Thread.sleep(sleepTime);
		} catch (InterruptedException consumed) {
			//don't care in this case, exiting anyway.
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		StructuredTransformer structProcess = new StructuredTransformer();
		structProcess.run();
	}

}
