package gov.ornl.stucco.workers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.ornl.stucco.ConfigLoader;
import gov.ornl.stucco.RabbitMQConsumer;
import gov.ornl.stucco.RabbitMQMessage;
import gov.ornl.stucco.RabbitMQProducer;
import gov.ornl.stucco.Util;
import gov.ornl.stucco.GraphConstructor;
import gov.ornl.stucco.Align;
import gov.ornl.stucco.preprocessors.PreprocessSTIX;
import gov.ornl.stucco.preprocessors.PreprocessSTIX.Vertex;
import gov.ornl.stucco.stix_extractors.ArgusExtractor;
import gov.ornl.stucco.stix_extractors.BugtraqExtractor;
import gov.ornl.stucco.stix_extractors.CaidaExtractor;
import gov.ornl.stucco.stix_extractors.CIF1d4Extractor;
import gov.ornl.stucco.stix_extractors.CIFZeusTrackerExtractor;
import gov.ornl.stucco.stix_extractors.CIFEmergingThreatsExtractor;
import gov.ornl.stucco.stix_extractors.CleanMxVirusExtractor;
import gov.ornl.stucco.stix_extractors.ClientBannerExtractor;
import gov.ornl.stucco.stix_extractors.CpeExtractor;
import gov.ornl.stucco.stix_extractors.CveExtractor;
import gov.ornl.stucco.stix_extractors.DNSRecordExtractor;
import gov.ornl.stucco.stix_extractors.FSecureExtractor;
import gov.ornl.stucco.stix_extractors.GeoIPExtractor;
import gov.ornl.stucco.stix_extractors.HoneExtractor;
import gov.ornl.stucco.stix_extractors.HTTPDataExtractor;
import gov.ornl.stucco.stix_extractors.LoginEventExtractor;
import gov.ornl.stucco.stix_extractors.MalwareDomainListExtractor;
import gov.ornl.stucco.stix_extractors.MetasploitExtractor;
import gov.ornl.stucco.stix_extractors.NvdToStixExtractor;
import gov.ornl.stucco.stix_extractors.PackageListExtractor;
import gov.ornl.stucco.stix_extractors.ServerBannerExtractor;
import gov.ornl.stucco.stix_extractors.ServiceListExtractor;
import gov.ornl.stucco.stix_extractors.SophosExtractor;
import gov.ornl.stucco.graph_extractors.ArgusGraphExtractor;
import gov.ornl.stucco.graph_extractors.HTTPDataGraphExtractor;
import gov.ornl.stucco.graph_extractors.HTTPRDataGraphExtractor;
import gov.ornl.stucco.graph_extractors.SituGraphExtractor;
import gov.ornl.stucco.graph_extractors.SnoGraphExtractor;
import gov.pnnl.stucco.doc_service_client.DocServiceClient;
import gov.pnnl.stucco.doc_service_client.DocServiceException;
import gov.pnnl.stucco.doc_service_client.DocumentObject;

import org.mitre.stix.stix_1.STIXPackage;
import org.mitre.cybox.cybox_2.Observables;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jdom2.Element;

public class StructuredWorker {
	private static final Logger logger = LoggerFactory.getLogger(StructuredWorker.class);
	private static final String PROCESS_NAME = "STRUCTURED";

	private static final String[] argusHeaders = {"StartTime", "Flgs", "Proto", "SrcAddr", "Sport", "Dir", "DstAddr", "Dport", "TotPkts", "TotBytes", "State"};

	private RabbitMQConsumer consumer;
	private RabbitMQProducer producer;

	private DocServiceClient docClient;

	private PreprocessSTIX preprocessSTIX;
	private GraphConstructor constructGraph;

	private boolean persistent;
	private int sleepTime;

	private final String HOSTNAME_KEY = "hostName";

	public StructuredWorker() {
		logger.info("loading config file from default location");
		ConfigLoader configLoader = new ConfigLoader();
		init(configLoader);
	}

	public StructuredWorker(String configFile) {
		logger.info("loading config file at: " + configFile);
		ConfigLoader configLoader = new ConfigLoader(configFile);
		init(configLoader);
	}

	private void init(ConfigLoader configLoader) {
		try {
			Map<String, Object> configMap;
			configMap = configLoader.getConfig("structured_data");

			persistent = Boolean.parseBoolean(String.valueOf(configMap.get("persistent")));
			sleepTime = Integer.parseInt(String.valueOf(configMap.get("emptyQueueSleepTime")));

			consumer = new RabbitMQConsumer(configMap);
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
		logger.info("RabbitMQ (structured_data) connected.");

		try {
			Map<String, Object> configMap;
			configMap = configLoader.getConfig("alignment_data");

			producer = new RabbitMQProducer(configMap);
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

					logger.debug("Recieved: " + routingKey + " deliveryTag=[" + messageID + "] message- "+ messageBody);

					//Get the document from the document server, if necessary
					String content = messageBody;
					if (!contentIncluded && !routingKey.endsWith(".sophos") && !routingKey.endsWith(".bugtraq")) {
						String docId = content.trim();
						logger.debug("Retrieving document content from Document-Service for id '" + docId + "'.");

						try {
							DocumentObject document = docClient.fetch(docId);
							String rawContent = document.getDataAsString();
							JSONObject jsonContent = new JSONObject(rawContent);
							content = (String) jsonContent.get("document");
						} catch (DocServiceException e) {
							logger.error("Could not fetch document '" + docId + "' from Document-Service.", e);
							logger.error("Message content was:\n"+messageBody);
						} catch (Exception e) {
							logger.error("Other error in handling document '" + docId + "' from Document-Service.", e);
							logger.error("Message content was:\n"+messageBody);
						}
					}

					//get a few other things from the message before passing to extractors.
					String docIDs = null;
					if (!contentIncluded) docIDs = messageBody;
					Map<String, String> metaDataMap = null;
					if (routingKey.endsWith(".hone")) {
						if ((headerMap != null) && (headerMap.containsKey(HOSTNAME_KEY))) {
							// The extractor needs Map<String,String>, and the headerMap is Map<String,Object>.
							// Also, the original headerMap may contain things that extractors don't care about.
							metaDataMap = new HashMap<String, String>();
							String hostname = String.valueOf(headerMap.get(HOSTNAME_KEY));
							metaDataMap.put(HOSTNAME_KEY, hostname);
						}
					}

					//Construct the subgraph by parsing the structured data
					JSONObject graph = generateGraph(routingKey, content, metaDataMap, docIDs);

					//TODO: Add timestamp into subgraph
					//Merge subgraph into full knowledge graph
					if (graph != null) {
						sendToAlignment(graph);
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
			producer.close();
		} catch (IOException e) {
			logger.error("Encountered RabbitMQ IO error when closing connection:", e);
			//don't care in this case, exiting anyway.
		}
	}

	private void sendToAlignment(JSONObject graph) {
		Map<String, JSONObject> components = Util.splitGraph(graph);
		for(String type: components.keySet()){
			Map<String, String> metadata = new HashMap<String,String>();
			metadata.put("contentType", "application/json");
			metadata.put("dataType", type);
			byte[] messageBytes = components.get(type).toString().getBytes();
			producer.sendContentMessage(metadata, messageBytes);
		}
	}

	/**
	 * @param routingKey determines which extractor to use
	 * @param content the text to parse
	 * @param metaDataMap any additional required info, which is not included in the content
	 * @param docIDs if the content is from the document server, this is its id(s).  Only included for debugging output.
	 * @return
	 */
	private JSONObject generateGraph(String routingKey, String content, Map<String, String> metaDataMap, String docIDs) {
		boolean stixDocument = false;
		STIXPackage stixPackage = null;
		JSONObject graph = null;

		try {
			if (routingKey.endsWith(".argus")) {
				ArgusGraphExtractor extractor = new ArgusGraphExtractor(argusHeaders, content);
				return extractor.getGraph();
			} else if (routingKey.endsWith(".http")) {
				//TODO: find name of http file ... for now (for testing) it just has .http extencion
				HTTPDataGraphExtractor httpExtractor = new HTTPDataGraphExtractor(content);
				return httpExtractor.getGraph();
			} else if (routingKey.endsWith(".httpr")) {
				//TODO: find name of http file ... for now (for testing) it just has .http extencion
				HTTPRDataGraphExtractor httprExtractor = new HTTPRDataGraphExtractor(content);
				return httprExtractor.getGraph();
			} else if (routingKey.endsWith(".situ")) {
				SituGraphExtractor situExtractor = new SituGraphExtractor(content);
				return situExtractor.getGraph();
			} else if (routingKey.endsWith(".sno")) {
				SnoGraphExtractor snoExtractor = new SnoGraphExtractor(content);
				return snoExtractor.getGraph();
			} else if (routingKey.endsWith(".cve")) {
				CveExtractor cveExtractor = new CveExtractor(content);
				stixPackage = cveExtractor.getStixPackage();
			} else if (routingKey.endsWith(".nvd")) {
				NvdToStixExtractor nvdExt = new NvdToStixExtractor(content);
				stixPackage = nvdExt.getStixPackage();
			} else if (routingKey.endsWith(".cpe")) {
				CpeExtractor cpeExtractor = new CpeExtractor(content);
				stixPackage = cpeExtractor.getStixPackage();
			} else if (routingKey.endsWith(".maxmind")) {
				GeoIPExtractor geoIPExtractor = new GeoIPExtractor(content);
				stixPackage = geoIPExtractor.getStixPackage();
			} else if (routingKey.endsWith(".metasploit")) {
				MetasploitExtractor metasploitExtractor = new MetasploitExtractor(content);
				stixPackage = metasploitExtractor.getStixPackage();
			} else if (routingKey.replaceAll("\\-", "").endsWith(".cleanmx")) {
				CleanMxVirusExtractor virusExtractor = new CleanMxVirusExtractor(content);
				stixPackage = virusExtractor.getStixPackage();
			} else if (routingKey.endsWith(".login_events")) {
				LoginEventExtractor loginEventExtractor = new LoginEventExtractor(content);
				stixPackage = loginEventExtractor.getStixPackage();
			} else if (routingKey.endsWith(".installed_package")) {
				PackageListExtractor packageListExtractor = new PackageListExtractor(content);
				stixPackage = packageListExtractor.getStixPackage();
			} else if (routingKey.endsWith(".1d4")){
				CIF1d4Extractor cifExtractor = new CIF1d4Extractor(content);
				stixPackage = cifExtractor.getStixPackage();
			} else if (routingKey.endsWith(".zeustracker")) {
				CIFZeusTrackerExtractor cifExtractor = new CIFZeusTrackerExtractor(content);
				stixPackage = cifExtractor.getStixPackage();
			} else if (routingKey.endsWith(".emergingthreats")) {
				CIFEmergingThreatsExtractor cifExtractor = new CIFEmergingThreatsExtractor(content);
				stixPackage = cifExtractor.getStixPackage();
			} else if (routingKey.endsWith(".servicelist")) {
				ServiceListExtractor serviceListExtractor = new ServiceListExtractor(content);
				stixPackage = serviceListExtractor.getStixPackage();
			} else if (routingKey.endsWith(".serverbanner")) {
				ServerBannerExtractor serverBannerExtractor = new ServerBannerExtractor(content);
				stixPackage = serverBannerExtractor.getStixPackage();
			} else if (routingKey.endsWith(".clientbanner")) {
				ClientBannerExtractor clientBannerExtractor = new ClientBannerExtractor(content);
				stixPackage = clientBannerExtractor.getStixPackage();
			} else if (routingKey.replaceAll("\\-", "").endsWith(".fsecure")) {
				FSecureExtractor fSecureExt = new FSecureExtractor(content);
				stixPackage = fSecureExt.getStixPackage();
			} else if (routingKey.endsWith(".malwaredomainlist")) {
				MalwareDomainListExtractor mdlExt = new MalwareDomainListExtractor(content);
				stixPackage = mdlExt.getStixPackage();
			} else if (routingKey.endsWith(".dnsrecord")) {
				DNSRecordExtractor dnsExt = new DNSRecordExtractor(content);
				stixPackage = dnsExt.getStixPackage();
			} else if (routingKey.endsWith(".hone")) {
				HoneExtractor honeExtractor = null;
				if ((metaDataMap != null) && (metaDataMap.containsKey(HOSTNAME_KEY))) {
					honeExtractor = new HoneExtractor(content, metaDataMap.get(HOSTNAME_KEY));
				} else {
					honeExtractor = new HoneExtractor(content);
				}
				stixPackage = honeExtractor.getStixPackage();
			} else if (routingKey.endsWith(".caida")) {
				//TODO: ensure file names match
				String as2org = null;
				String pfx2as = null;
				String[] items = content.split("\\r?\\n");
				for (String item : items) {
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
						logger.error("Complete message content was:\n" + content);
						return null;
					}
					if (sourceURL.contains("as2org")) {
						as2org = itemContent;
					} else if (sourceURL.contains("pfx2as")) {
						pfx2as = itemContent;
					} else {
						logger.warn("unexpected URL (sophos) " + sourceURL);
					}
				}
				if (as2org != null && pfx2as != null) {
					CaidaExtractor caidaExtractor = new CaidaExtractor(as2org, pfx2as);
					stixPackage = caidaExtractor.getStixPackage();
				}
			} else if (routingKey.endsWith(".sophos")) {
				String summary = null;
				String details = null;
				String[] items = content.split("\\r?\\n");
				for (String item : items) {
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
						logger.error("Complete message content was:\n" + content);
						return null;
					}
					if (sourceURL.contains("/detailed-analysis.aspx")) {
						details = itemContent;
					} else if(sourceURL.contains(".aspx")) {
						summary = itemContent;
					} else {
						logger.warn("unexpected URL (sophos) " + sourceURL);
					}
				}
				if (summary != null && details != null) {
					SophosExtractor sophosExt = new SophosExtractor(summary, details);
					stixPackage = sophosExt.getStixPackage();
				} else {
					logger.warn("Sophos: some required fields were null, skipping group.\nMessage was:" + content);
				}
			} else if (routingKey.endsWith(".bugtraq")) {
				String info = null;
				String discussion = null;
				String exploit = null;
				String solution = null;
				String references = null;
				String[] items = content.split("\\r?\\n");
				for (String item : items) {
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
						logger.error("Complete message content was:\n" + content);
						return null;
					}
					if (sourceURL.contains("/info")) {
						info = itemContent;
					} else if (sourceURL.contains("/discuss")) { //interestingly, "/discuss" and "/discussion" are both valid urls for this item
						discussion = itemContent;
					} else if (sourceURL.contains("/exploit")) {
						exploit = itemContent;
					} else if (sourceURL.contains("/solution")) {
						solution = itemContent;
					} else if (sourceURL.contains("/references")) {
						references = itemContent;
					} else {
						logger.warn("unexpected URL (bugtraq) " + sourceURL);
					}
				}
				if (info != null && discussion != null && exploit != null && solution != null && references != null) {
					BugtraqExtractor bugtraqExt = new BugtraqExtractor(info, discussion, exploit, solution, references);
					stixPackage = bugtraqExt.getStixPackage();
				} else {
					logger.warn("Bugtraq: some required fields were null, skipping group.\nMessage was:" + content);
					if (docIDs != null) {
						logger.error("Problem docid(s):\n" + docIDs);
					}
				}
			} else if (routingKey.endsWith(".stix")) {
				stixDocument = true;
			} else {
				logger.warn("Unexpected routing key encountered '" + routingKey + "'.");
			}

			if (stixPackage != null) {
				Map<String, Vertex> stixElements = preprocessSTIX.normalizeSTIX(stixPackage.toXMLString());
				graph = constructGraph.constructGraph(stixElements);
			} else if (stixDocument) {
				Map<String, Vertex> stixElements = preprocessSTIX.normalizeSTIX(content);
				graph = constructGraph.constructGraph(stixElements);
			} else {
				logger.warn("Unexpected null stix package for routing key '" + routingKey + "'.");
			}
		} catch (RuntimeException e) {
			logger.error("Error occurred with routingKey = " + routingKey);
			logger.error("										docIDs = " + docIDs);
			logger.error("										content = " + content);
			e.printStackTrace();
			return null;
		}

		return graph;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		StructuredWorker structProcess;
		if (args.length == 0) {
			structProcess = new StructuredWorker();
		}
		else {
			structProcess = new StructuredWorker(args[0]);
		}
		structProcess.run();
	}
}