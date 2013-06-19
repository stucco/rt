package gov.ornl.stucco.rt.spout

import backtype.storm.spout.Scheme
import backtype.storm.tuple.Fields

import java.util.{List => JList}

import grizzled.slf4j.Logging

class Deserializer extends Scheme with Logging {
  override def deserialize(ser: Array[Byte]): JList[AnyRef] = null

  override def getOutputFields(): Fields = null
}
