package storm.base.exampletest

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import storm.base.exampletest.Doubler._

@RunWith(classOf[JUnitRunner])
class DoublerSuite extends FunSuite {
  
  test("double: 3") {
    assert(double(3) === 6)
  }

  test("double: 5") {
    assert(double(5) === 10)
  }
}
