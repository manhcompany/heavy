package com.heavy.etl.utils

import com.heavy.core.utils.Scheduler
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

class SchedulerTest extends FlatSpec {

  behavior of "SchedulerTest"

  it should "test config" in {
    val config = Config.loadConfig("etl")
    val operators = config.operators.map(x => SparkOperator(x))
    Scheduler.execute[DataFrame](operators)
  }
}
