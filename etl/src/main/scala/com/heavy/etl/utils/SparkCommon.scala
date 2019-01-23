package com.heavy.etl.utils

import com.heavy.etl.monitor.SparkQueryListenerInterceptor
import com.heavy.etl.udfs.SparkUdfInterceptor
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkCommon {

  def getSparkContext: SparkContext = {
    SparkContext.getOrCreate
  }

  var spark: SparkSession = _

  def getSparkSession(appName: String = "SparkApplication"): SparkSession = {
    if(spark == null) {
      spark = SparkSession
        .builder()
        .appName(appName)
        .enableHiveSupport()
        .getOrCreate()
      SparkUdfInterceptor.intercept(spark)
      SparkQueryListenerInterceptor.intercept(spark)
    }
    spark
  }
}