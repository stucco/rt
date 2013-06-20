package gov.ornl.stucco.rt.spout

import backtype.storm.spout.Scheme
import backtype.storm.tuple.Fields

import java.util.{List => JList}
import scala.collection.JavaConversions._

import grizzled.slf4j.Logging

class Deserializer extends Scheme with Logging {
  override def deserialize(ser: Array[Byte]): JList[AnyRef] =
    List(new String(ser))

  override def getOutputFields(): Fields = new Fields("json")
}
