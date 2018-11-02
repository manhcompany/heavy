package com.heavy.etl.apps

import com.heavy.core.utils.{Operator, Scheduler}
import com.heavy.etl.utils.{Config, ETLConfig, SparkOperator}
import org.apache.spark.sql.DataFrame

object ETL extends App {
  override def main(args: Array[String]): Unit = {
    val path = args(0)
    val config = Config.loadConfig(path, "etl")
    val operators = config.operators.foldLeft(List[Operator[DataFrame]]())((oprs, x) => oprs.::(SparkOperator(x)))
    Scheduler.execute[DataFrame](operators)
  }
}