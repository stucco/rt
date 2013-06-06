import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import Doubler._

@RunWith(classOf[JUnitRunner])
class DoublerSuite extends FunSuite {
  
  test("double: 3") {
    assert(double(3) === 6)
  }

  test("double: 5") {
    assert(double(5) === 10)
  }
}
