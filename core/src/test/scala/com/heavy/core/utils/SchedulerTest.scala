package com.heavy.core.utils

import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

class SchedulerTest extends FlatSpec {

  behavior of "SchedulerTest"

  it should "execute" in {
    val config = Config.loadConfig("test_spark_operator_factory_input.conf", "etl")
    val operators = config.operators.map(x => SparkOperator(x))
    Scheduler.execute[DataFrame](operators)
  }

  it should "write" in {
    val config = Config.loadConfig("test_spark_operator_factory_input.conf", "etl")
    val operators = config.operators.map(x => SparkOperator(x))
    Scheduler.execute[DataFrame](operators)
  }

  it should "join" in {
    val config = Config.loadConfig("test_spark_join.conf", "etl")
    val operators = config.operators.map(x => SparkOperator(x))
    Scheduler.execute[DataFrame](operators)
  }

  it should "union" in {
    val config = Config.loadConfig("test_spark_union.conf", "etl")
    val operators = config.operators.map(x => SparkOperator(x))
    Scheduler.execute[DataFrame](operators)
  }

  it should "dedup" in {
    val config = Config.loadConfig("test_spark_dedup.conf", "etl")
    val operators = config.operators.map(x => SparkOperator(x))
    Scheduler.execute[DataFrame](operators)
  }

  it should "drop" in {
    val config = Config.loadConfig("test_spark_drop.conf", "etl")
    val operators = config.operators.map(x => SparkOperator(x))
    Scheduler.execute[DataFrame](operators)
  }

  it should "rename" in {
    val config = Config.loadConfig("test_spark_rename.conf", "etl")
    val operators = config.operators.map(x => SparkOperator(x))
    Scheduler.execute[DataFrame](operators)
  }

  it should "alias" in {
    val config = Config.loadConfig("test_spark_alias.conf", "etl")
    val operators = config.operators.map(x => SparkOperator(x))
    Scheduler.execute[DataFrame](operators)
  }
}
