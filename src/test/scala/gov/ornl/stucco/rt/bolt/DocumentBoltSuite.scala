import org.scalatest.FunSuite

import gov.ornl.stucco.rt.bolt.DocumentBolt

import backtype.storm.tuple.Values

class DocumentBoltSuite extends FunSuite {

  val bolt = new DocumentBolt

  test("document storing works correctly") {
    (pending)
  }
}
