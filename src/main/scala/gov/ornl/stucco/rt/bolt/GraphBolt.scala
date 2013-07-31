package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Fields, Tuple, Values }
import java.util.{ Map => JMap }

import grizzled.slf4j.Logging

/**
 * A bolt that merges a subgraph into the graph database.
 */
class GraphBolt extends BaseRichBolt with Logging {

  private var collector: OutputCollector = _

  /**
   * Process a tuple by merging its graph into the database.
   */
  def process(uuid: String, graph: String) = {
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
