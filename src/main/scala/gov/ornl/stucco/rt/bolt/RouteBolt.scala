package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{Fields, Tuple, Values}
import java.util.{Map => JMap}

import grizzled.slf4j.Logging

/** A bolt that sends a tuple to the appropriate pipeline, depending on whether
  * the tuple contains a structured document or unstructured document.
  */
class RouteBolt extends BaseRichBolt with Logging {

  private var collector: OutputCollector = _

  /** Determines the stream that a tuple should be sent to.
    *
    * If a tuple contains a structured document, the corresponding Stream Id is
    * "structured", and if it contains an unstructured document, the Stream Id
    * is "unstructured". This decision is based on a field in the JSON string.
    *
    * @param json A JSON string containing the tuple's data.
    *
    * @return A string containing the StreamId of the destination pipeline.
    */
  def streamId(json: String) = {
    // decide if "structured" or "unstructured"
    // "structured"
    "structured"
  }

  /** Process a tuple (identity transformation).
    *
    * This bolt is responsible for only routing, so it does not need to modify
    * the ''data'' in the tuple, only the ''destination''.
    */
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
    collector.emit(streamId(json), tuple, process(uuid, json))
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declareStream("structured", new Fields("uuid", "json"))
    declarer.declareStream("unstructured", new Fields("uuid", "json"))
  }
}
