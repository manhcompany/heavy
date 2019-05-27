package com.heavy.etl.apps

import com.heavy.core.stackmachine.CanonicalStackMachine
import com.heavy.core.utils.Logging
import com.heavy.etl.utils.{Config, SparkOperatorFactory}
import org.apache.spark.sql.DataFrame

object ETL extends Logging{
  def main(args: Array[String]): Unit = {
    val config = Config.loadConfig("etl")
    val operators = config.operators.map(x => SparkOperatorFactory.factory(x).get)
    log.info(operators.toString())
    CanonicalStackMachine.execute[DataFrame](operators)
  }
}