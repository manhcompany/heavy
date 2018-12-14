package com.heavy.etl.utils

import com.heavy.etl.udfs.SparkUdfInterceptor
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkCommon {

  def getSparkContext: SparkContext = {
    SparkContext.getOrCreate()
  }

  def getSparkSession: SparkSession = {
    val spark = SparkSession.builder().getOrCreate()
    SparkUdfInterceptor.intercept(spark)
    spark
  }
}