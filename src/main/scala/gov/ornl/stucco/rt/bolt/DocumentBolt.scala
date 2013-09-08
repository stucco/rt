package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Fields, Tuple, Values }
import java.util.{ Map => JMap }

import grizzled.slf4j.Logging

/**
 * A bolt that inserts a tuple's document into the document store.
 */
class DocumentBolt extends BaseRichBolt with Logging {

  private var collector: OutputCollector = _

  /**
   * Process a tuple by inserting its document into the document store.
   */
  def process(uuid: String, graph: String) {
    // insert into document store
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
    val graph = tuple getStringByField "graph" //TODO field name will differ based on source, need to handle or change.
    process(uuid, graph)
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    // no outputs
  }
}
