package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Fields, Tuple, Values }
import java.util.{ Map => JMap }

import grizzled.slf4j.Logging

/**
 * A bolt to be used as a template when writing other bolts.
 */
class TemplateBolt extends BaseRichBolt with Logging {

  private var collector: OutputCollector = _

  def process() = {
    new Values()
  }

  override def prepare(config: JMap[_, _],
    context: TopologyContext,
    collector: OutputCollector) {
    info("preparing for operation")
    this.collector = collector
  }

  override def execute(tuple: Tuple) {
    debug(s"executing tuple: $tuple")
    collector.emit(tuple, process())
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields())
  }
}
