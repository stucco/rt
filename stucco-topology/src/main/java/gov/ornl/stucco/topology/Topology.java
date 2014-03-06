package gov.ornl.stucco.topology;

import gov.ornl.stucco.bolt.AlignmentBolt;
import gov.ornl.stucco.bolt.ConceptBolt;
import gov.ornl.stucco.bolt.ParseBolt;
import gov.ornl.stucco.bolt.RelationBolt;
import gov.ornl.stucco.bolt.UUIDBolt;
import gov.ornl.stucco.spout.RabbitMQTopicSpout;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;


public class Topology {
	private static final Logger logger = LoggerFactory.getLogger(Topology.class);
	private static final String CONFIG_FILE_NAME = "config.yaml";
	private static final int DEFAULT_INSTANCES = 2;
	
	private enum BOLTS {
		STRUCTURED_DATA,
		UNSTRUCTURED_DATA,
		UUID_STRUCT,
		PARSE,
		UUID_UNSTRUCT,
		CONCEPT,
		RELATION,
		ALIGNMENT
	}
	
	public static final String EMPTY_GRAPHSON = "{ \"edges\":[], \"vertices\":[] }";
	
	private TopologyBuilder builder;
	private Map<String, Object> configMap;
	
	@SuppressWarnings("unchecked")
	public Topology() {
		builder = new TopologyBuilder();
		
		Yaml yaml = new Yaml();
		//read in yaml file as json object
		configMap = (Map<String, Object>) yaml.load(Topology.class.getClassLoader().getResourceAsStream(CONFIG_FILE_NAME));
		Integer structSpoutInstances = DEFAULT_INSTANCES;
		Integer structUuidInstances = DEFAULT_INSTANCES;
		Integer parseInstances = DEFAULT_INSTANCES;
		Integer unstructSpoutInstances = DEFAULT_INSTANCES;
		Integer unstructUuidInstances = DEFAULT_INSTANCES;
		Integer conceptInstances = DEFAULT_INSTANCES;
		Integer realtionInstances = DEFAULT_INSTANCES;
		Integer alignmentInstances = DEFAULT_INSTANCES;
		try {
			structSpoutInstances = ((Map<String, Integer>) configMap.get("instances")).get(BOLTS.STRUCTURED_DATA.toString().toLowerCase());
			structUuidInstances = ((Map<String, Integer>) configMap.get("instances")).get(BOLTS.UUID_STRUCT.toString().toLowerCase());
			parseInstances = ((Map<String, Integer>) configMap.get("instances")).get(BOLTS.PARSE.toString().toLowerCase());
			unstructSpoutInstances = ((Map<String, Integer>) configMap.get("instances")).get(BOLTS.UNSTRUCTURED_DATA.toString().toLowerCase());
			unstructUuidInstances = ((Map<String, Integer>) configMap.get("instances")).get(BOLTS.UUID_UNSTRUCT.toString().toLowerCase());
			conceptInstances = ((Map<String, Integer>) configMap.get("instances")).get(BOLTS.CONCEPT.toString().toLowerCase());
			realtionInstances = ((Map<String, Integer>) configMap.get("instances")).get(BOLTS.RELATION.toString().toLowerCase());
			alignmentInstances = ((Map<String, Integer>) configMap.get("instances")).get(BOLTS.ALIGNMENT.toString().toLowerCase());
		} catch (Exception ex) {
			logger.warn("Could not find instances in config file, defaulting to '" + DEFAULT_INSTANCES + "' instances.");
		}
		
		//build the topology
		//create structured data topic spout
		//TODO check for non-null spout
		RabbitMQTopicSpout structSpout = buildSpout(BOLTS.STRUCTURED_DATA.toString().toLowerCase());
		builder.setSpout(BOLTS.STRUCTURED_DATA.toString().toLowerCase(), structSpout, structSpoutInstances);
		logger.debug("spout [" + BOLTS.STRUCTURED_DATA.toString().toLowerCase() + "] built with " + structSpoutInstances + " instances.");
		
		builder.setBolt(BOLTS.UUID_STRUCT.toString().toLowerCase(), (new UUIDBolt()), structUuidInstances).shuffleGrouping(BOLTS.STRUCTURED_DATA.toString().toLowerCase());
		logger.debug("bolt [" + BOLTS.UUID_STRUCT.toString().toLowerCase() + "] built with " + structUuidInstances + " instances.");
		
		builder.setBolt(BOLTS.PARSE.toString().toLowerCase(), (new ParseBolt()), parseInstances).shuffleGrouping(BOLTS.UUID_STRUCT.toString().toLowerCase());
		logger.debug("bolt [" + BOLTS.PARSE.toString().toLowerCase() + "] built with " + parseInstances + " instances.");
		
		//create unstructured data topic spout
		//TODO check for non-null spout
		RabbitMQTopicSpout unstructSpout = buildSpout(BOLTS.UNSTRUCTURED_DATA.toString().toLowerCase());
		builder.setSpout(BOLTS.UNSTRUCTURED_DATA.toString().toLowerCase(), unstructSpout, unstructSpoutInstances);
		logger.debug("spout [" + BOLTS.UNSTRUCTURED_DATA.toString().toLowerCase() + "] built with " + unstructSpoutInstances + " instances.");
		
		builder.setBolt(BOLTS.UUID_UNSTRUCT.toString().toLowerCase(), (new UUIDBolt()), unstructUuidInstances).shuffleGrouping(BOLTS.UNSTRUCTURED_DATA.toString().toLowerCase());
		logger.debug("bolt [" + BOLTS.UUID_UNSTRUCT.toString().toLowerCase() + "] built with " + unstructUuidInstances + " instances.");
		
		builder.setBolt(BOLTS.CONCEPT.toString().toLowerCase(), (new ConceptBolt()), conceptInstances).shuffleGrouping(BOLTS.UUID_UNSTRUCT.toString().toLowerCase());
		logger.debug("bolt [" + BOLTS.CONCEPT.toString().toLowerCase() + "] built with " + conceptInstances + " instances.");
		
		builder.setBolt(BOLTS.RELATION.toString().toLowerCase(), (new RelationBolt()), realtionInstances).shuffleGrouping(BOLTS.CONCEPT.toString().toLowerCase());
		logger.debug("bolt [" + BOLTS.RELATION.toString().toLowerCase() + "] built with " + realtionInstances + " instances.");
		
		builder.setBolt(BOLTS.ALIGNMENT.toString().toLowerCase(), (new AlignmentBolt()), alignmentInstances).shuffleGrouping(BOLTS.PARSE.toString().toLowerCase()).shuffleGrouping(BOLTS.RELATION.toString().toLowerCase());
		logger.debug("bolt [" + BOLTS.ALIGNMENT.toString().toLowerCase() + "] built with " + alignmentInstances + " instances.");
	}
	
	
	@SuppressWarnings("unchecked")
	public RabbitMQTopicSpout buildSpout(String spoutName) {
		RabbitMQTopicSpout spout = null;
		if (configMap.containsKey(spoutName)) {
			try {
				String exchangeName = ((Map<String, String>) configMap.get(spoutName)).get("exchange");
				String host = ((Map<String, String>) configMap.get(spoutName)).get("host");
				Integer port = ((Map<String, Integer>) configMap.get(spoutName)).get("port");
				String username = ((Map<String, String>) configMap.get(spoutName)).get("username");
				String password = ((Map<String, String>) configMap.get(spoutName)).get("password");
				String queueName = ((Map<String, String>) configMap.get(spoutName)).get("queue");
				List<String> bindings = ((Map<String, List<String>>) configMap.get(spoutName)).get("bindings");
				String[] bindingKeys = new String[bindings.size()];
				bindingKeys = bindings.toArray(bindingKeys);
				spout = new RabbitMQTopicSpout(exchangeName, queueName, host, port, username, password, bindingKeys);
                
				logger.debug("SPOUT CREATED on " + exchangeName + " - " + queueName + " with bindings '" + bindingKeys + "'");
			} catch (Exception ex) {
				logger.error("Error reading config file.", ex);
			}
		}
		return spout;
	}
	
	
	@SuppressWarnings("unchecked")
	public void runTopology(String[] args) {
		//set up the storm configuration
		Boolean debug = ((Map<String, Boolean>) configMap.get("storm")).get("debug");
		if (debug == null) {
			debug = Boolean.TRUE;
		}
		Config config = new Config();
		config.setDebug(debug);
        
		logger.debug("DEBUG MODE is on.");
		
		if (args.length > 0) {
			Integer workers = ((Map<String, Integer>) configMap.get("storm")).get("workers");
			if (workers == null) {
				workers = new Integer(DEFAULT_INSTANCES);
			}
			config.setNumWorkers(workers.intValue());
            
			logger.debug("Preparing to submit topology with configuration of " + workers + " worker instances.");
            
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			} catch (AlreadyAliveException e) {
				logger.error("Topology is already running.", e);
			} catch (InvalidTopologyException e) {
				logger.error("Attempted to submit an invalid topology.", e);
			}
		}
		else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("RT-Topology", config, builder.createTopology());
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Topology topo = new Topology();
		topo.runTopology(args);
	}

}
