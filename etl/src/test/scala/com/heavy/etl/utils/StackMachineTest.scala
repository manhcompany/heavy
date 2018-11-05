package com.heavy.etl.utils

import com.heavy.core.stackmachine.StackMachine
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

class StackMachineTest extends FlatSpec {

  behavior of "SchedulerTest"

  it should "test config" in {
    val config = Config.loadConfig("etl")
    val operators = config.operators.map(x => SparkOperator(x))
    StackMachine.execute[DataFrame](operators)
  }
}
