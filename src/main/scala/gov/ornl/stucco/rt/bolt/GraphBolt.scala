package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Fields, Tuple, Values }
import java.util.{ Map => JMap }

import grizzled.slf4j.Logging
import grizzled.slf4j.Logger

import gov.ornl.stucco.rt.util.Loader

/**
 * A bolt that merges a subgraph into the graph database.
 */
class GraphBolt extends BaseRichBolt with Logging {

  private var collector: OutputCollector = _

  /**
   * Process a tuple by merging its graph into the database.
   */
  def process(uuid: String, graph: String) = {
    val dbLocation = "/usr/local/neo4j-community-1.9.2/data/graph.db"
    val loggerRef = Logger(getClass.getName.replace("$", "#").stripSuffix("#")).logger
    val loader = new Loader()
    loader.load(graph, dbLocation)
    // insert into graph db
  }

  override def prepare(config: JMap[_, _],
    context: TopologyContext,
    collector: OutputCollector) {
    info("preparing for operation")
    this.collector = collector
  }

  override def execute(tuple: Tuple) {
    debug(s"executing tuple: $tuple")
    val uuid = tuple getStringByField "uuid"
    val graph = tuple getStringByField "graph"
    process(uuid, graph)
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    // no outputs
  }
}
