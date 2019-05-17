package com.heavy.etl.utils

import com.heavy.core.stackmachine.{Operator, UnaryOperator}
import org.apache.spark.sql.DataFrame

import scala.util.Try

class DataValidationOperator extends SparkOperatorFactory {

  class DescribeOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    val columns: Seq[String] = Seq("time_stamp", "date_time", "dataset", "column_name", "key", "value")
    val timestamp: Long = System.currentTimeMillis / 1000
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      val sqlContext = SparkCommon.getSparkSession().sqlContext
      import sqlContext.implicits._
      val cols = config.describeCols.map(x => x.map(x => x.col)).get
      val describeResult = operands.head.describe(cols = cols: _*)
      describeResult.cache()
      val result = config.describeCols.map(x => x.map(describeOpt => {
        val colName = describeOpt.col
        val summaries = describeOpt.summary

        summaries.tail.foldLeft(
          Seq((timestamp, config.date, config.dataset, colName, summaries.head,
            describeResult.select(colName).filter(s"summary = '${summaries.head}'").first().get(0).asInstanceOf[String].toDouble)).toDF(columns: _*)
        )((result, summary) => result.union(Seq((timestamp, config.date, config.dataset, colName, summary,
          describeResult.select(colName).filter(s"summary = '$summary'").first().get(0).asInstanceOf[String].toDouble)).toDF(columns: _*)))
      })).map(x => x.reduce((a, b) => a.union(b)))
      describeResult.unpersist()
      result.map(x => List(x))
    }
  }

  override def factory(config: OperatorConfig): Option[Operator[DataFrame]] = {
    Try(Some(new ShowDataFrame(
      config.name match {
        case "describe" => new DescribeOperator(config)
      }))
    ).map(d => d).recover { case _: Throwable => None }.get
  }
}
