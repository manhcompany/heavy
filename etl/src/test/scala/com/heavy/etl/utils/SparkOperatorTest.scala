package com.heavy.etl.utils

import com.heavy.etl.utils.SparkOperator.InputOperator
import org.scalatest.FlatSpec

class SparkOperatorTest extends FlatSpec {

  behavior of "SparkOperatorTest"

  it should "apply" in {
    val config = Config.loadConfig("etl")

    assertResult(true) {
      SparkOperator(config.operators.head).isInstanceOf[InputOperator]
    }
  }
}
