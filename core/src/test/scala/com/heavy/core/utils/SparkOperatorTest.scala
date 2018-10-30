package com.heavy.core.utils

import com.heavy.core.utils.SparkOperator.InputOperator
import org.scalatest.FlatSpec

class SparkOperatorTest extends FlatSpec {

  behavior of "SparkOperatorTest"

  it should "apply" in {
    val config = Config.loadConfig("test_spark_operator_factory_input.conf", "etl")

    assertResult(true) {
      SparkOperator(config.operators(0)).isInstanceOf[InputOperator]
    }
  }
}
