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

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String content = tuple.getStringByField("message");
		
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
		
		Values values = new Values(uuid.toString(), tuple.getStringByField("source"), tuple.getLongByField("timestamp"), tuple.getBooleanByField("contentIncl"), content);
		logger.debug("emitting " + values);
		collector.emit(tuple, values);
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uuid", "source", "timestamp", "contentIncl", "message"));
	}

}
