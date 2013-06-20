package gov.ornl.stucco.rt.bolt

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{Fields, Tuple, Values}

import java.util.{Map => JMap}
import java.security.MessageDigest

import grizzled.slf4j.Logging

class UUIDBolt extends BaseRichBolt with Logging {
  private var collector: OutputCollector = _

  def hash(s: String) = {
    val bytes = MessageDigest getInstance "SHA-512" digest s.getBytes
    ("" /: bytes) { (str, byte) => str + f"$byte%02x" }
    }

  def process(json: String) = {
    new Values(hash(json), json)
  }
  
  override def prepare(config: JMap[_, _],
      context: TopologyContext,
      collector: OutputCollector) {
    info("preparing for operation")
    this.collector = collector
  }

  override def execute(tuple: Tuple) {
    debug(s"executing tuple: $tuple")
    val json = tuple getStringByField "json"
    collector.emit(tuple, process(json))
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("uuid", "json"))
  }
}
