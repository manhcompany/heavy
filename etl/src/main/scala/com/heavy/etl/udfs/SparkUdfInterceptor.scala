package com.heavy.etl.udfs

import java.util.ServiceLoader

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

trait SparkUdfInterceptor {
  def intercept(spark: SparkSession): Unit
}

object SparkUdfInterceptor extends SparkUdfInterceptor {

  def intercept(spark: SparkSession): Unit = {
    ServiceLoader.load(classOf[SparkUdfInterceptor])
      .asScala
      .toList
      .map(_.getClass)
      .map(x => x.newInstance()).foreach(x => x.intercept(spark))
  }
}

