package gov.ornl.stucco.bolt;

import gov.ornl.stucco.topology.Topology;

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

public class RelationBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(RelationBolt.class);
	
	private OutputCollector collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}


	@Override
	public void execute(Tuple tuple) {
		String uuid = tuple.getStringByField("uuid");
		String message = tuple.getStringByField("message");
		String concepts = tuple.getStringByField("concepts");
		String graph = Topology.EMPTY_GRAPHSON;
		
		if (!tuple.getBooleanByField("contentIncl")) {
			//TODO: get document from doc-service
		}
		
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
