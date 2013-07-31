package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Fields, Tuple, Values }
import java.util.{ Map => JMap }

import grizzled.slf4j.Logging

/**
 * A bolt that performs relation extraction and computes the corresponding
 * subgraph.
 */
class RelationBolt extends BaseRichBolt with Logging {

  private var collector: OutputCollector = _

  /**
   * Process a tuple by performing relation extraction and computing
   * the graph.
   *
   * @return `Values` containing the UUID and the graph.
   */
  def process(uuid: String, text: String, concepts: String) = {
    // perform relations extraction
    val relations = "relations..."
    val graph = "graph..."
    new Values(uuid, graph)
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
    val text = tuple getStringByField "text"
    val concepts = tuple getStringByField "concepts"
    collector.emit(tuple, process(uuid, text, concepts))
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("uuid", "graph"))
  }
}
