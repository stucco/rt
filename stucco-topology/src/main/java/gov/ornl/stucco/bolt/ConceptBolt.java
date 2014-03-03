package gov.ornl.stucco.bolt;

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

import gov.ornl.stucco.entity.EntityExtractor;

public class ConceptBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(ConceptBolt.class);
	
	private OutputCollector collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String uuid = tuple.getStringByField("uuid");
		String text = tuple.getStringByField("message");
		if (!tuple.getBooleanByField("contentIncl")) {
			//TODO: get document from doc-service
			text = "document id encountered";
		}
		String concepts = "";
		try {
			EntityExtractor extractor = new EntityExtractor();
			concepts = extractor.getAnnotatedTextAsJson(text);
		} catch (Exception e) {
			logger.error("Error loading entity-extractor models.", e);
		}
		
		Values values = new Values(uuid, tuple.getLongByField("timestamp"), tuple.getBooleanByField("contentIncl"), tuple.getStringByField("message"), concepts);
		logger.debug("emitting " + values);
		collector.emit(tuple, values);
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uuid", "timestamp", "contentIncl", "message", "concepts"));
	}
}
