package com.heavy.assert_and_unit_test

import org.scalatest.FlatSpec

class AssertAndUnitTest extends FlatSpec{
  it should "assert" in {
    def add(x: Int, y: Int) = {
      x + y ensuring(x > 0)
    }
    val as = Seq(1, 2, 3, 0, 4, 5, -1, 8, 9)
    assert(as.length == 9)

    add(1, 10)
    val a = Array.range(1, 100, 1).map(_ / 100.0)//    assertResult(1)(1)

    val b = s"array(${a.mkString(",")})"
    println(b)

  }
}
