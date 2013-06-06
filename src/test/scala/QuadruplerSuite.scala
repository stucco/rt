import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import Quadrupler._

@RunWith(classOf[JUnitRunner])
class QuadruplerSuite extends FunSuite {

  test("quadruple: 4") {
    assert(quadruple(4) === 16)
  }

  test("quadruple: 0") {
    assert(quadruple(0) === 0)
  }
}
