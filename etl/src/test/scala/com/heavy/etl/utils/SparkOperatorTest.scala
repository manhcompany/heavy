package com.heavy.etl.utils

import com.heavy.core.stackmachine.Operator
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

class SparkOperatorTest extends FlatSpec {

  behavior of "SparkOperatorTest"

  it should "apply" in {
    val config = Config.loadConfig("etl")

    assertResult(true) {
      SparkOperatorFactory.factory(config.operators.head).isInstanceOf[Operator[DataFrame]]
    }
  }
}
