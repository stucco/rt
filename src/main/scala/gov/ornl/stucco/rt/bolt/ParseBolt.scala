package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{Fields, Tuple, Values}

import java.util.{Map => JMap}
import java.security.MessageDigest

import grizzled.slf4j.Logging

/** A bolt that parses a structured document and produces its
  * corresponding subgraph.
  */
class ParseBolt extends BaseRichBolt with Logging {

  private var collector: OutputCollector = _

  /** Process a tuple by computing its subgraph.
    *
    * @return `Values` containing the UUID and a subgraph.
    */
  def process(uuid: String, json: String): Values = {
    // perform parsing
    val graph = "text here..."
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
    val json = tuple getStringByField "json"
    collector.emit(tuple, process(uuid, json))
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("uuid", "graph"))
  }
}
