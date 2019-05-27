package com.heavy.etl.utils

import com.heavy.core.stackmachine.{Operator, UnaryOperator}
import org.apache.spark.sql.DataFrame

import scala.util.Try

class DataValidationOperator extends SparkOperatorFactory {

  class DescribeOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    val columns: Seq[String] = Seq("time_stamp", "date_time", "dataset", "column_name", "key", "value")
    val timestamp: Long = System.currentTimeMillis / 1000
    override def execute(operands: Option[DataFrame]*): Either[String, Option[List[DataFrame]]] = {
      val sqlContext = SparkCommon.getSparkSession().sqlContext
      import sqlContext.implicits._
      val cols = config.describeCols.map(x => x.map(x => x.col)).get
      val describeResult = operands.head.get.describe(cols = cols: _*)
      describeResult.cache()
      val result = config.describeCols.map(x => x.map(describeOpt => {
        val colName = describeOpt.col
        val summaries = describeOpt.summary

        summaries.map(summary => {
          val value = Try(describeResult.select(colName).filter(s"summary = '$summary'").first().get(0).asInstanceOf[String].toDouble)
            .getOrElse(null.asInstanceOf[Double])
          Seq((timestamp, config.date, config.dataset, s"desc_$colName", summary, value
          )).toDF(columns: _*)}).reduce(_ union _)
      })).map(x => x.reduce(_ union _))
      describeResult.unpersist()
      Right(result.map(x => List(x)))
    }
  }

  class FacetOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    val columns: Seq[String] = Seq("time_stamp", "date_time", "dataset", "column_name", "key", "value")
    val timestamp: Long = System.currentTimeMillis / 1000

    override def execute(operands: Option[DataFrame]*): Either[String, Option[List[DataFrame]]] = {
      val result = config.cols.map(col => col.map(c => {
        operands.head.get.groupBy(c).count.selectExpr(
          s"'$timestamp' as time_stamp",
          s"'${config.date.get}' as date_time",
          s"'${config.dataset.get}' as dataset",
          s"'facet_$c' as column_name",
          s"$c as key",
          "count as value")
      }).reduce(_ union _))
      Right(result.map(x => List(x)))
    }
  }

  override def factory(config: OperatorConfig): Option[Operator[DataFrame]] = {
    Try(Some(new ShowDataFrame(
      config.name match {
        case "describe" => new DescribeOperator(config)
        case "facet" => new FacetOperator(config)
      }))
    ).map(d => d).recover { case _: Throwable => None }.get
  }
}
