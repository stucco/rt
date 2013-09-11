package gov.ornl.stucco.rt.bolt

import morph.ast._
import morph.ast.Implicits._
import morph.ast.DSL._
import morph.parser._
import morph.parser.Interface._
import morph.utils.Utils._
import morph.parser._
import gov.ornl.stucco.extractors._

import backtype.storm.task.{ OutputCollector, TopologyContext }
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.{ Fields, Tuple, Values }

import java.util.{ Map => JMap }
import java.security.MessageDigest

import grizzled.slf4j.Logging

/**
 * A bolt that parses a structured document and produces its
 * corresponding subgraph.
 */
class ParseBolt extends BaseRichBolt with Logging {

  private var collector: OutputCollector = _

  /**
   * Process a tuple by computing its subgraph.
   *
   * @return `Values` containing the UUID and a subgraph.
   */
  def process(uuid: String, json: String): Values = {
    // perform parsing
    val jsonObj = JsonParser(json) // using morph to get these from the json string...
    val sourceType = (jsonObj ~> "source" ~> "name").asString
    val data = (jsonObj ~> "content").asString
    val graph = sourceType match {
      case "cve" => CveExtractor(XmlParser(data)).toString
      case "nvd" => NvdExtractor(XmlParser(data)).toString
      //TODO: add more cases here
      case _ => "{ \"edges\":[], \"vertices\":[] }" //TODO: default case, what to do?
    }
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
    info("got json: " + json)
    print("got this json thing: " + json + "\n")
    collector.emit(tuple, process(uuid, json))
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("uuid", "graph"))
  }
}
