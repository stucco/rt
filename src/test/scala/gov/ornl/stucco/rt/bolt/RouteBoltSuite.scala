import org.scalatest.FunSuite

import gov.ornl.stucco.rt.bolt.RouteBolt

import backtype.storm.tuple.Values

class RouteBoltSuite extends FunSuite {

  val bolt = new RouteBolt

  test("stream correctly determined for structured data") {
    (pending)
  }

  test("stream correctly determined for unstructured data") {
    (pending)
  }

  test("process doesn't modify tuple") {
    val result = bolt process ("", "")
    assert(result === new Values("", ""))
  }
}
