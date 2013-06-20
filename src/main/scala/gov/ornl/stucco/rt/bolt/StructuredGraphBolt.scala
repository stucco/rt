package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{Fields, Tuple, Values}
import java.util.{Map => JMap}

import grizzled.slf4j.Logging

class StructuredGraphBolt extends BaseRichBolt with Logging {
  private var collector: OutputCollector = _

  def process(uuid: String, chunk: String) = {
    // perform graph building
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
    val chunk = tuple getStringByField "chunk"
    collector.emit(tuple, process(uuid, chunk))
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("uuid", "graph"))
  }
}
