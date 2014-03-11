package gov.ornl.stucco.bolt;

import gov.ornl.stucco.entity.EntityExtractor;
import gov.ornl.stucco.entity.models.Sentence;
import gov.ornl.stucco.entity.models.Sentences;

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

public class ConceptBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(ConceptBolt.class);
	
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
		String content = tuple.getStringByField("message");
		
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
		
		String concepts = "";
		try {
			EntityExtractor extractor = new EntityExtractor();
			concepts = extractor.getAnnotatedTextAsJson(content);
			
		} catch (Exception e) {
			logger.error("Error loading entity-extractor models.", e);
		}
		
		Values values = new Values(uuid, tuple.getStringByField("source"), tuple.getLongByField("timestamp"), tuple.getBooleanByField("contentIncl"), tuple.getStringByField("message"), concepts.toString());
//		logger.debug("emitting " + values);
		collector.emit(tuple, values);
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uuid", "source", "timestamp", "contentIncl", "message", "concepts"));
	}
}
