package gov.ornl.stucco.rt.spout

import backtype.storm.spout.Scheme
import backtype.storm.tuple.Fields

import java.util.{ List => JList }
import scala.collection.JavaConversions._

import grizzled.slf4j.Logging

/**
 * A component that can deserialize binary data from a message queue.
 */
class Deserializer extends Scheme with Logging {

  /**
   * Deserializes binary data into a string
   *
   * @param ser Binary data to deserialize.
   *
   * @return A list with a single string containing the data.
   */
  override def deserialize(ser: Array[Byte]): JList[AnyRef] =
    List(new String(ser))

  /**
   * Declares the output fields of this component.
   */
  override def getOutputFields(): Fields = new Fields("json")
}
