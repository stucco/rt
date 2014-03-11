package gov.ornl.stucco.bolt;

import gov.ornl.stucco.entity.models.Sentence;
import gov.ornl.stucco.entity.models.Sentences;
import gov.ornl.stucco.entity.models.Word;
import gov.ornl.stucco.topology.Topology;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RelationBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(RelationBolt.class);
	
	private ObjectMapper mapper = new ObjectMapper();
	private OutputCollector collector;
//	private DocServiceClient docClient;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
//		if ((stormConf.containsKey(Topology.DOC_SERVICE_CLIENT_HOST)) && (stormConf.containsKey(Topology.DOC_SERVICE_CLIENT_PORT))) {
//			String host = (String) stormConf.get(Topology.DOC_SERVICE_CLIENT_HOST);
//			int port = ((Long) stormConf.get(Topology.DOC_SERVICE_CLIENT_PORT)).intValue();
//			this.docClient = new DocServiceClient(host, port);
//		}
//		else {
//			this.docClient = new DocServiceClient();
//		}
	}


	@Override
	public void execute(Tuple tuple) {
		String uuid = tuple.getStringByField("uuid");
		String dataSource = tuple.getStringByField("source");
		String content = tuple.getStringByField("message");
		String concepts = tuple.getStringByField("concepts");
		String graph = Topology.EMPTY_GRAPHSON;
		
		if (!tuple.getBooleanByField("contentIncl")) {
			String docId = content.trim();
			logger.debug("Retrieving document content from Document-Service.");
			
//			if (docClient != null) {
//				try {
//					DocumentObject document = docClient.fetch(docId);
//					content = document.getDataAsString();
//				} catch (DocServiceException e) {
//					logger.error("Could not fetch document from Document-Service.", e);
//				}
//			}
//			else {
//				logger.warn("Can't retrieve documents because Document-Service client is null.");
//			}
		}
		
		//TODO: relation extraction with the content
		StringBuilder verticesBuilder = new StringBuilder();
		verticesBuilder.append("{\"vertices\":[");
		StringBuilder edgesBuilder = new StringBuilder();
		edgesBuilder.append("\"edges\":[");

		Map<String, Map<String, String>> vertexMap = new HashMap<String, Map<String, String>>();
		try {
			Sentences sentences = mapper.readValue(concepts, Sentences.class);
			List<Sentence> sentenceList = sentences.getSentenceList();
			for (int k=0; k<sentenceList.size(); k++) {
				Sentence sent = sentenceList.get(k);
				
				List<Word> wordList = sent.getWordList();
				for (int i=0; i<wordList.size(); i++) {
					Word word = wordList.get(i);
					
					if (word.getDomainLabel().equalsIgnoreCase("sw.vendor")) {
						
						StringBuilder vendor = new StringBuilder();
						vendor.append(word.getWord());
						while ((i+1 < wordList.size()) && (wordList.get(i+1).getDomainLabel().equalsIgnoreCase("sw.vendor"))) {
							Word nextWord = wordList.get(i+1);
							vendor.append(" ");
							vendor.append(nextWord.getWord());
							i = i + 1;
						}
						
						Map<String, String> vendorVertex = new HashMap<String, String>();
						vendorVertex.put("vendor", vendor.toString());
						vertexMap.put(String.valueOf(k), vendorVertex);
					}
					
				}
			}
		} catch (IOException e) {
			logger.error("Could not parse concepts.", e);
		}
		edgesBuilder.append("] }");
		verticesBuilder.append("], ");
		
		verticesBuilder.append(edgesBuilder.toString());
		graph = verticesBuilder.toString();
		
		Values values = new Values(uuid, graph, tuple.getLongByField("timestamp"));
		logger.debug("emitting " + values);
		collector.emit(tuple, values);
		collector.ack(tuple);
	}
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uuid", "graph", "timestamp"));
	}

}
