package gov.ornl.stucco.bolt;

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
import gov.ornl.stucco.morph.parser.XmlParser;
import gov.ornl.stucco.morph.parser.ParsingException;
import gov.ornl.stucco.topology.Topology;
import gov.pnnl.stucco.doc_service_client.DocServiceClient;
import gov.pnnl.stucco.doc_service_client.DocServiceException;
import gov.pnnl.stucco.doc_service_client.DocumentObject;

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
		String uuid = tuple.getStringByField("uuid");
		String dataSource = tuple.getStringByField("source");
		boolean contentIncl = tuple.getBooleanByField("contentIncl");
		String content = tuple.getStringByField("message");
		String graph = Topology.EMPTY_GRAPHSON;
		
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
		
		if (dataSource.contains(".cve")) {
			ValueNode parsedData = null;
			try{
				ValueNode nodeData = XmlParser.apply(content);
				parsedData = CveExtractor.extract(nodeData);
			} catch (ParsingException e) {
				logger.error("ParsingException in parsing cve!", e);
			} catch (Exception e) {
				logger.error("Other Error in parsing cve!", e);
			}
			if(parsedData != null){
				graph = String.valueOf(parsedData);
			}
		}
		else if (dataSource.contains(".nvd")) {
			ValueNode parsedData = null;
			try{
				ValueNode nodeData = XmlParser.apply(content);
				parsedData = NvdExtractor.extract(nodeData);
			} catch (ParsingException e) {
				logger.error("ParsingException in parsing nvd!", e);
			} catch (Exception e) {
				logger.error("Other Error in parsing nvd!", e);
			}
			if(parsedData != null){
				graph = String.valueOf(parsedData);
			}
		}
		else if (dataSource.contains(".cpe")) {
			ValueNode parsedData = null;
			try{
				ValueNode nodeData = XmlParser.apply(content);
				parsedData = CpeExtractor.extract(nodeData);
			} catch (ParsingException e) {
				logger.error("ParsingException in parsing cpe!", e);
			} catch (Exception e) {
				logger.error("Other Error in parsing cpe!", e);
			}
			if(parsedData != null){
				graph = String.valueOf(parsedData);
			}
		}
		else if (dataSource.contains(".maxmind")) {
			ValueNode parsedData = null;
			try{
				ValueNode nodeData = CsvParser.apply(content);
				parsedData = GeoIPExtractor.extract(nodeData);
			} catch (ParsingException e) {
				logger.error("ParsingException in parsing maxmind!", e);
			} catch (Exception e) {
				logger.error("Other Error in parsing maxmind!", e);
			}
			if(parsedData != null){
				graph = String.valueOf(parsedData);
			}
		}
		else if (dataSource.contains(".argus")) {
			ValueNode parsedData = null;
			try{
				ValueNode nodeData = XmlParser.apply(content);
				parsedData = ArgusExtractor.extract(nodeData);
			} catch (ParsingException e) {
				logger.error("ParsingException in parsing argus!", e);
			} catch (Exception e) {
				logger.error("Other Error in parsing argus!", e);
			}
			if(parsedData != null){
				graph = String.valueOf(parsedData);
			}
		}
		else if (dataSource.contains(".hone")) {
			ValueNode parsedData = null;
			try{
				ValueNode nodeData = CsvParser.apply(content);
				parsedData = HoneExtractor.extract(nodeData);
			} catch (ParsingException e) {
				logger.error("ParsingException in parsing hone!", e);
			} catch (Exception e) {
				logger.error("Other Error in parsing hone!", e);
			}
			if(parsedData != null){
				graph = String.valueOf(parsedData);
			}
		}
		else if (dataSource.contains(".metasploit")) {
			ValueNode parsedData = null;
			try{
				ValueNode nodeData = CsvParser.apply(content);
				parsedData = MetasploitExtractor.extract(nodeData);
			} catch (ParsingException e) {
				logger.error("ParsingException in parsing metasploit!", e);
			} catch (Exception e) {
				logger.error("Other Error in parsing metasploit!", e);
			}
			if(parsedData != null){
				graph = String.valueOf(parsedData);
			}
		}
		else if (dataSource.contains(".cleanmx")) {
			ValueNode parsedData = null;
			try{
				ValueNode nodeData = XmlParser.apply(content);
				parsedData = CleanMxVirusExtractor.extract(nodeData);
			} catch (ParsingException e) {
				logger.error("ParsingException in parsing cleanmx!", e);
			} catch (Exception e) {
				logger.error("Other Error in parsing cleanmx!", e);
			}
			if(parsedData != null){
				graph = String.valueOf(parsedData);
			}
		}
		else {
			logger.warn("Unexpected routing key encountered '" + dataSource + "'.");
		}
		String singleLine = graph;
		//logger.debug(singleLine.replaceAll("\n", ""));
		
		Values values = new Values(uuid, graph, tuple.getLongByField("timestamp"));
		//logger.debug("emitting " + values);
		collector.emit(tuple, values);
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uuid", "graph", "timestamp"));
	}
}
