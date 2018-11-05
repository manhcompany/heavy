package com.heavy.etl.apps

import com.heavy.core.stackmachine.{Operator, StackMachine}
import com.heavy.etl.utils.{Config, SparkOperator}
import org.apache.spark.sql.DataFrame

object ETL {
  def main(args: Array[String]): Unit = {
    val config = Config.loadConfig("etl")
    val operators = config.operators.foldLeft(List[Operator[DataFrame]]())((oprs, x) => oprs.::(SparkOperator(x)))
    StackMachine.execute[DataFrame](operators)
  }
}