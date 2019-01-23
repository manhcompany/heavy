package com.heavy.etl.monitor

import java.util.ServiceLoader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.JavaConverters._

trait SparkQueryListenerInterceptor {
  def intercept(spark: SparkSession): Unit
}

object SparkQueryListenerInterceptor extends SparkQueryListenerInterceptor {
  override def intercept(spark: SparkSession): Unit = {
    ServiceLoader.load(classOf[QueryExecutionListener])
      .asScala
      .toList
      .map(_.getClass())
      .map(x => x.newInstance())
      .foreach(x => spark.sessionState.listenerManager.register(x))
  }
}
