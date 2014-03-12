package gov.ornl.stucco.bolt;

import gov.ornl.stucco.topology.Topology;
import gov.pnnl.stucco.doc_service_client.DocServiceClient;
import gov.pnnl.stucco.doc_service_client.DocServiceException;
import gov.pnnl.stucco.doc_service_client.DocumentObject;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UUIDBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(UUIDBolt.class);
	
	private OutputCollector collector;
	private DocServiceClient docClient;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
			if ((stormConf.containsKey(Topology.DOC_SERVICE_CLIENT_HOST)) && (stormConf.containsKey(Topology.DOC_SERVICE_CLIENT_PORT))) {
				String host = (String) stormConf.get(Topology.DOC_SERVICE_CLIENT_HOST);
				int port = ((Long) stormConf.get(Topology.DOC_SERVICE_CLIENT_PORT)).intValue();
				this.docClient = new DocServiceClient(host, port);
			}
			else {
				this.docClient = new DocServiceClient();
			}
	}

	@Override
	public void execute(Tuple tuple) {
		String content = tuple.getStringByField("message");
		boolean contentIncl = tuple.getBooleanByField("contentIncl");
		if (!contentIncl) {
			String docId = content.trim();
			logger.debug("Retrieving document content from Document-Service for id '" + docId + "'.");
			
			if (docClient != null) {
				try {
					DocumentObject document = docClient.fetch(docId);
					content = document.getDataAsString();
				} catch (DocServiceException e) {
					logger.error("Could not fetch document from Document-Service.", e);
				}
			}
			else {
				logger.warn("Can't retrieve documents because Document-Service client is null.");
			}
		}
		
		UUID uuid = UUID.nameUUIDFromBytes(content.getBytes());
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("SHA-1");
			md.update(content.getBytes());
			byte[] messageBytes = md.digest();
			uuid = UUID.nameUUIDFromBytes(messageBytes);
		} catch (NoSuchAlgorithmException ex) {
			logger.error("UUID algorithm cannot be found.", ex);
		}
		
		Values values = new Values(uuid.toString(), tuple.getStringByField("source"), tuple.getLongByField("timestamp"), contentIncl, content);
		logger.debug("emitting " + values);
		collector.emit(tuple, values);
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uuid", "source", "timestamp", "contentIncl", "message"));
	}

}
