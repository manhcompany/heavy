package com.heavy.etl.utils

import com.heavy.etl.utils.SparkOperator.InputOperator
import org.scalatest.FlatSpec

class SparkOperatorTest extends FlatSpec {

  behavior of "SparkOperatorTest"

  it should "apply" in {
    val config = Config.loadConfig("test_spark_operator_factory_input.conf", "etl")

    assertResult(true) {
      SparkOperator(config.operators.head).isInstanceOf[InputOperator]
    }
  }
}
