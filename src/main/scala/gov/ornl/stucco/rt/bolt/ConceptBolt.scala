package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{Fields, Tuple, Values}
import java.util.{Map => JMap}

import grizzled.slf4j.Logging

/** A bolt that extracts concepts from unstructured text.
  */
class ConceptBolt extends BaseRichBolt with Logging {

  private var collector: OutputCollector = _

  /** Process a tuple by appending extracted concepts.
    *
    * @return `Values` containing the UUID, text, and extracted concepts.
    */
  def process(uuid: String, text: String): Values = {
    // perform concept extraction
    val concepts = "concepts..."
    new Values(uuid, text, concepts)
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
    collector.emit(tuple, process(uuid, text))
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("uuid", "text", "concepts"))
  }
}
