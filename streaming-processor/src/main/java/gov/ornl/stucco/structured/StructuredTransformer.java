package gov.ornl.stucco.structured;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
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
import gov.ornl.stucco.extractors.SituCyboxExtractor;
import gov.ornl.stucco.extractors.CIF1d4Extractor;
import gov.ornl.stucco.extractors.CIFZeusTrackerExtractor;
import gov.ornl.stucco.extractors.CIFEmergingThreatsExtractor;
import gov.ornl.stucco.extractors.ServiceListExtractor;
import gov.ornl.stucco.extractors.ServerBannerExtractor;
import gov.ornl.stucco.extractors.ClientBannerExtractor;
import gov.ornl.stucco.morph.ast.ValueNode;
import gov.ornl.stucco.morph.parser.CsvParser;
import gov.ornl.stucco.morph.parser.ParsingException;
import gov.ornl.stucco.morph.parser.XmlParser;
import gov.pnnl.stucco.doc_service_client.DocServiceClient;
import gov.pnnl.stucco.doc_service_client.DocServiceException;
import gov.pnnl.stucco.doc_service_client.DocumentObject;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alignment.alignment_v2.Align;
import HTMLExtractor.FSecureExtractor;
import HTMLExtractor.MalwareDomainListExtractor;
import HTMLExtractor.SophosExtractor;
import HTMLExtractor.BugtraqExtractor;
import HTMLExtractor.DNSRecordExtractor;

import com.rabbitmq.client.GetResponse;

public class StructuredTransformer {
	private static final Logger logger = LoggerFactory.getLogger(StructuredTransformer.class);
	private static final String PROCESS_NAME = "STRUCTURED";

	private RabbitMQConsumer consumer;

	private DocServiceClient docClient;
	private Align alignment;
	
	private boolean persistent;
	private int sleepTime;
	
	private final String HOSTNAME_KEY = "hostName";
	
	public StructuredTransformer() {
		logger.info("loading config file from default location");
		ConfigLoader configLoader = new ConfigLoader();
		init(configLoader);
	}
	
	public StructuredTransformer(String configFile) {
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
			configMap = configLoader.getConfig("structured_data");
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
			
			logger.info("DB connection created.  Connecting to document service...");
			configMap = configLoader.getConfig("document_service");

			host = String.valueOf(configMap.get("host"));
			port = Integer.parseInt(String.valueOf(configMap.get("port")));
			docClient = new DocServiceClient(host, port);
		} catch (IOException e) {
			logger.error("Error initializing Alignment and/or DB connection.", e);
			System.exit(-1);
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
				long itemStartTime = System.currentTimeMillis();
				String routingKey = response.getEnvelope().getRoutingKey().toLowerCase();
				long deliveryTag = response.getEnvelope().getDeliveryTag();
				
				String message = "";
				if (response.getBody() != null) {
					message = new String(response.getBody());
					
					/*long timestamp = 0;
					if (response.getProps().getTimestamp() != null) {
						timestamp = response.getProps().getTimestamp().getTime();
					}*/
	
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
							String rawContent = document.getDataAsString();
							JSONObject jsonContent = new JSONObject(rawContent);
							content = (String) jsonContent.get("document"); 
						} catch (DocServiceException e) {
							logger.error("Could not fetch document '" + docId + "' from Document-Service.", e);
							logger.error("Message content was:\n"+message);
						} catch (Exception e) {
							logger.error("Other error in handling document '" + docId + "' from Document-Service.", e);
							logger.error("Message content was:\n"+message);
						}
					}
					
					//get a few other things from the message before passing to extractors.
					String docIDs = null;
					if (!contentIncluded) docIDs = message;
					Map<String, String> metaDataMap = null;
					if (routingKey.contains(".hone")) {
						if ((headerMap != null) && (headerMap.containsKey(HOSTNAME_KEY))) {
							// The extractor needs Map<String,String>, and the headerMap is Map<String,Object>.
							// Also, the original headerMap may contain things that extractors don't care about.
							metaDataMap = new HashMap<String, String>();
							String hostname = String.valueOf(headerMap.get(HOSTNAME_KEY));
							metaDataMap.put(HOSTNAME_KEY, hostname);
						}
					}
					
					//Construct the subgraph by parsing the structured data	
					String graph = generateGraph(routingKey, content, metaDataMap, docIDs);

					//TODO: Add timestamp into subgraph
					//Merge subgraph into full knowledge graph
					if(graph != null) alignment.load(graph);
					
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
				
				long itemEndTime = System.currentTimeMillis();
				logger.debug( "Finished processing item in " + (itemEndTime - itemStartTime) + " ms. " +
						" routingKey: " + routingKey + " deliveryTag: " + deliveryTag + " message: " + message);

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
			try{
				Thread.sleep(sleepTime);
			} catch (InterruptedException consumed) {
				//don't care in this case, exiting anyway.
			}
		}while(persistent && !fatalError);
		try{
			consumer.close();
		} catch (IOException e) {
			logger.error("Encountered RabbitMQ IO error when closing connection:", e);
			//don't care in this case, exiting anyway.
		}
	}
	
	
	/**
	 * @param routingKey determines which extractor to use
	 * @param content the text to parse
	 * @param metaDataMap any additional required info, which is not included in the content
	 * @param docIDs if the content is from the document server, this is its id(s).  Only included for debugging output.
	 * @return
	 */
	private String generateGraph(String routingKey, String content, Map<String, String> metaDataMap, String docIDs) {
		String graph = null;
		String source = null;
		boolean parserDone = false;
		
		//handle the simple morph cases...
		if (routingKey.contains(".cve")) {
			source = "cve";
		}else if (routingKey.contains(".nvd")) {
			source = "nvd";
		}else if (routingKey.contains(".cpe")) {
			source = "cpe";
		}else if (routingKey.contains(".maxmind")) {
			source = "maxmind";
		}else if (routingKey.contains(".argus")) {
			source = "argus";
		}else if (routingKey.contains(".metasploit")) {
			source = "metasploit";
		}else if (routingKey.replaceAll("\\-", "").contains(".cleanmx")) {
			source = "cleanmx";
		}else if (routingKey.contains(".login_events")) {
			source = "login_events";
		}else if (routingKey.contains(".installed_package")) {
			source = "installed_package";
		}else if (routingKey.contains("situ")) {
			source = "situ";
		}else if (routingKey.contains("1d4")) {
			source = "1d4";
		}else if (routingKey.contains("zeustracker")) {
			source = "zeustracker";
		}else if (routingKey.contains("emergingthreats")) {
			source = "emergingthreats";
		}else if (routingKey.contains(".servicelist")) {
			source = "servicelist";
		}else if (routingKey.contains(".serverbanner")) {
			source = "serverbanner";
		}else if (routingKey.contains(".clientbanner")) {
			source = "clientbanner";
		}
		
		if(source != null && parserDone == false){
			ValueNode parsedData = null;
			try{
				if (source.equals("cve")) {
					ValueNode nodeData = XmlParser.apply(content);
					parsedData = (ValueNode) CveExtractor.extract(nodeData);
				}else if(source.equals("nvd")){
					ValueNode nodeData = XmlParser.apply(content);
					parsedData = (ValueNode) NvdExtractor.extract(nodeData);
				}else if(source.equals("cpe")){
					ValueNode nodeData = XmlParser.apply(content);
					parsedData = (ValueNode) CpeExtractor.extract(nodeData);
				}else if(source.equals("maxmind")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) GeoIPExtractor.extract(nodeData);
				}else if(source.equals("argus")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) ArgusExtractor.extract(nodeData);
				}else if(source.equals("metasploit")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) MetasploitExtractor.extract(nodeData);
				}else if(source.equals("cleanmx")){
					ValueNode nodeData = XmlParser.apply(content);
					parsedData = (ValueNode) CleanMxVirusExtractor.extract(nodeData);
				}else if(source.equals("login_events")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) LoginEventExtractor.extract(nodeData);
				}else if(source.equals("installed_package")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) PackageListExtractor.extract(nodeData);
				}else if(source.equals("situ")){
					ValueNode nodeData = XmlParser.apply(content);
					parsedData = (ValueNode) SituCyboxExtractor.extract(nodeData);
				}else if(source.equals("1d4")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) CIF1d4Extractor.extract(nodeData);
				}else if(source.equals("zeustracker")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) CIFZeusTrackerExtractor.extract(nodeData);
				}else if(source.equals("emergingthreats")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) CIFEmergingThreatsExtractor.extract(nodeData);
				}else if(source.equals("servicelist")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) ServiceListExtractor.extract(nodeData);
				}else if(source.equals("serverbanner")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) ServerBannerExtractor.extract(nodeData);
				}else if(source.equals("clientbanner")){
					ValueNode nodeData = CsvParser.apply(content);
					parsedData = (ValueNode) ClientBannerExtractor.extract(nodeData);
				}
			} catch (ParsingException e) {
				logger.error("ParsingException in parsing " + source + "!", e);
				if (docIDs != null) logger.error("Problem docid(s):\n" + docIDs);
				else logger.error("Problem content was:\n"+content);
				graph = null;
			} catch (NullPointerException e) {
				logger.debug("null pointer in parsing " + source + "!", e);
				if (docIDs != null) logger.error("Problem docid(s):\n" + docIDs);
				else logger.debug("Problem content was:\n"+content);
				graph = null; 
			} catch (Exception e) {
				logger.error("Other Error in parsing " + source + "!", e);
				if (docIDs != null) logger.error("Problem docid(s):\n" + docIDs);
				else logger.error("Problem content was:\n"+content);
				graph = null;
			}
			if(parsedData != null){
				graph = String.valueOf(parsedData);
			}
			parserDone = true;
		}
		
		//handle the simple java parser cases
		if (routingKey.replaceAll("\\-", "").contains(".fsecure")) {
			source = "fsecure";
		}else if (routingKey.contains(".malwaredomainlist")) {
			source = "malwaredomainlist";
		}else if (routingKey.contains(".dnsrecord")) {
			source = "dnsrecord";
		}
		
		if(source != null && parserDone == false){
			try {
				if (source.equals("fsecure")) {
					FSecureExtractor fSecureExt = new FSecureExtractor(content);
					graph = fSecureExt.getGraph().toString();
				}else if(source.equals("malwaredomainlist")){
					MalwareDomainListExtractor mdlExt = new MalwareDomainListExtractor(content);
					graph = mdlExt.getGraph().toString();
				}else if(source.equals("dnsrecord")){
					DNSRecordExtractor dnsExt = new DNSRecordExtractor(content);
					graph = dnsExt.getGraph().toString();
				}
			} catch (NullPointerException e) {
				//TODO: revisit this.  //see DNS record extractor
				logger.debug("null pointer in parsing " + source + ".  (This can happen when the record has no useful info.)", e);
				if (docIDs != null) logger.error("Problem docid(s):\n" + docIDs);
				else logger.debug("Problem content was:\n"+content);
				graph = null;
			} catch (Exception e) {
				logger.error("Other Error in parsing " + source + "!", e);
				if (docIDs != null) logger.error("Problem docid(s):\n" + docIDs);
				else logger.error("Problem content was:\n"+content);
				graph = null;
			}
			parserDone = true;
		}

		//handle the remaining complex cases
		if(parserDone == false){
			if (routingKey.contains(".hone")) {
				ValueNode parsedData = null;
				try{
					ValueNode nodeData = CsvParser.apply(content);
					
					if ((metaDataMap != null) && (metaDataMap.containsKey(HOSTNAME_KEY))) {
						parsedData = (ValueNode) HoneExtractor.extract(nodeData, metaDataMap);
					} else {
						parsedData = (ValueNode) HoneExtractor.extract(nodeData);
					}
		
				} catch (ParsingException e) {
					logger.error("ParsingException in parsing hone!", e);
					if (docIDs != null) logger.error("Problem docid(s):\n" + docIDs);
					else logger.error("Problem content was:\n"+content);
					graph = null;
				} catch (Exception e) {
					logger.error("Other Error in parsing hone!", e);
					if (docIDs != null) logger.error("Problem docid(s):\n" + docIDs);
					else logger.error("Problem content was:\n"+content);
					graph = null;
				}
				if(parsedData != null){
					graph = String.valueOf(parsedData);
				}
				parserDone = true;
			}
			else if (routingKey.contains(".sophos")) {
				String summary = null;
				String details = null;
				try{
					String[] items = content.split("\\r?\\n");
					for(String item : items){
						String docId = item.split("\\s+")[0];
						String sourceURL = item.split("\\s+")[1];
						String rawItemContent = null;
						String itemContent = null;
						try {
							DocumentObject document = docClient.fetch(docId);
							rawItemContent = document.getDataAsString();
							JSONObject jsonContent = new JSONObject(rawItemContent);
							itemContent = (String) jsonContent.get("document"); 
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
				} catch (Exception e) {
					logger.error("Error in parsing sophos!", e);
					if (docIDs != null) logger.error("Problem docid(s):\n" + docIDs);
					else logger.error("Problem content was:\n"+content);
					graph = null;
				}
				parserDone = true;
			}
			else if (routingKey.contains(".bugtraq")) {
				String info = null;
				String discussion = null;
				String exploit = null;
				String solution = null;
				String references = null;
				try{
					String[] items = content.split("\\r?\\n");
					for(String item : items){
						String docId = item.split("\\s+")[0];
						String sourceURL = item.split("\\s+")[1];
						String rawItemContent = null;
						String itemContent = null;
						try {
							DocumentObject document = docClient.fetch(docId);
							rawItemContent = document.getDataAsString();
							JSONObject jsonContent = new JSONObject(rawItemContent);
							itemContent = (String) jsonContent.get("document");
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
				} catch (Exception e) {
					logger.error("Error in parsing bugtraq!", e);
					if (docIDs != null) logger.error("Problem docid(s):\n" + docIDs);
					else logger.error("Problem content was:\n"+content);
					graph = null;
				}
				parserDone = true;
			}
		}
		
		if(parserDone == false){
			logger.warn("Unexpected routing key encountered '" + routingKey + "'.");
		}
		return graph;
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
