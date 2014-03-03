package gov.ornl.stucco.bolt;

import gov.ornl.stucco.morph.ast.StringNode;
import gov.ornl.stucco.morph.ast.ValueNode;
import gov.ornl.stucco.extractors.ArgusExtractor;
import gov.ornl.stucco.extractors.CpeExtractor;
import gov.ornl.stucco.extractors.CveExtractor;
import gov.ornl.stucco.extractors.GeoIPExtractor;
import gov.ornl.stucco.extractors.HoneExtractor;
import gov.ornl.stucco.extractors.NvdExtractor;
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


public class ParseBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(ParseBolt.class);
	
	private OutputCollector collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String uuid = tuple.getStringByField("uuid");
		String dataSource = tuple.getStringByField("source");
		boolean contentIncl = tuple.getBooleanByField("contentIncl");
		String message = tuple.getStringByField("message");
		String graph = Topology.EMPTY_GRAPHSON;
		
		if (contentIncl) {
			if (dataSource.contains(".cve")) {
				StringNode stringNode = CveExtractor.String2StringNode(message);
				ValueNode nodeData = CveExtractor.extract(stringNode);
				graph = String.valueOf(nodeData);
			}
			else if (dataSource.contains(".nvd")) {
				StringNode stringNode = NvdExtractor.String2StringNode(message);
				ValueNode nodeData = NvdExtractor.extract(stringNode);
				graph = String.valueOf(nodeData);
			}
			else if (dataSource.contains(".cpe")) {
				StringNode stringNode = CpeExtractor.String2StringNode(message);
				ValueNode nodeData = CpeExtractor.extract(stringNode);
				graph = String.valueOf(nodeData);
			}
			else if (dataSource.contains(".maxmind")) {
				StringNode stringNode = GeoIPExtractor.String2StringNode(message);
				ValueNode nodeData = GeoIPExtractor.extract(stringNode);
				graph = String.valueOf(nodeData);
			}
			else if (dataSource.contains(".argus")) {
				StringNode stringNode = ArgusExtractor.String2StringNode(message);
				ValueNode nodeData = ArgusExtractor.extract(stringNode);
				graph = String.valueOf(nodeData);
			}
			else if (dataSource.contains(".hone")) {
				StringNode stringNode = HoneExtractor.String2StringNode(message);
				ValueNode nodeData = HoneExtractor.extract(stringNode);
				graph = String.valueOf(nodeData);
			}
			else {
				logger.warn("Unexpected routing key encountered '" + dataSource + "'.");
			}
		}
		else {
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
