package gov.ornl.stucco.rt.spout

import backtype.storm.spout.Scheme
import backtype.storm.tuple.Fields

import java.util.{List => JList}

class Deserializer extends Scheme {
  override def deserialize(ser: Array[Byte]): JList[AnyRef] = null

  override def getOutputFields(): Fields = null
}
