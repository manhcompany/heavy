package com.heavy.core.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkCommon {

  def getSparkContext: SparkContext = {
    SparkContext.getOrCreate()
  }

  def getSparkSession: SparkSession = {
    SparkSession.builder().getOrCreate()
  }
}