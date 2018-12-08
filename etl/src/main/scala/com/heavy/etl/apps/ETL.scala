package com.heavy.etl.apps

import com.heavy.core.stackmachine.{Operator, StackMachine}
import com.heavy.core.utils.Logging
import com.heavy.etl.utils.{Config, SparkOperator, SparkOperatorFactory}
import org.apache.spark.sql.DataFrame

object ETL extends Logging{
  def main(args: Array[String]): Unit = {
    val config = Config.loadConfig("etl")
//    val operators = config.operators.foldLeft(List[Operator[DataFrame]]())((oprs, x) => oprs.::(SparkOperator(x)))
    val operators = config.operators.map(x => SparkOperatorFactory.factory(x).get)
    log.info(operators.toString())
    StackMachine.execute[DataFrame](operators)
  }
}