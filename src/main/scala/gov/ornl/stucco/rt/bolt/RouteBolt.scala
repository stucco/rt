package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{Fields, Tuple, Values}
import java.util.{Map => JMap}

import grizzled.slf4j.Logging

class RouteBolt extends BaseRichBolt with Logging {
  private var collector: OutputCollector = _

  def streamId(tuple: Tuple) = {
    // check if structured or unstructured
    // "structured"
    "unstructured"
  }

  def process(uuid: String, json: String) = {
    new Values(uuid, json)
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
    collector.emit(streamId(tuple), tuple, process(uuid, json))
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declareStream("structured", new Fields())
    declarer.declareStream("unstructured", new Fields())
  }
}
