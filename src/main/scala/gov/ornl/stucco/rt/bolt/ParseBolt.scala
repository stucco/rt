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
    val message = JsonParser(json)
    val sourceType : String = (message ~> "source" ~> "name").asString
    val data : String = (message ~> "content").asString
    val contentType : String = (message ~> "contentType").asString
    debug(s"sourceType: $sourceType")
    debug(s"data: $data")
    debug(s"contentType: $contentType")
    //match extractor to data source
    val graph = sourceType match {
      case "cve" => CveExtractor(XmlParser(data)).toString
      case "nvd" => NvdExtractor(XmlParser(data)).toString
      case "argus" => ArgusExtractor(XmlParser(data)).toString
      case "cpe" => CpeExtractor(XmlParser(data)).toString
//      case "hone" => HoneExtractor(CsvParser(data)).toString
      case "maxmind" => GeoIPExtractor(CsvParser(data)).toString
      case _ => "{ \"edges\":[], \"vertices\":[] }"
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
    collector.emit(tuple, process(uuid, json))
    collector.ack(tuple)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("uuid", "graph"))
  }
}
