package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import scala.collection.mutable


class CustomSQLListenerSource(sc: SparkContext) extends Source with Logging {
  override val sourceName: String = sc.appName

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  def register(metrics: mutable.Map[String, Long]): Unit = {
    metrics.foreach(metric => {
      try {
        metricRegistry.register(metric._1, new Gauge[Long] {
          override def getValue: Long = metric._2
        })
      } catch {
        case _: IllegalArgumentException =>
          metricRegistry.remove(metric._1)
          metricRegistry.register(metric._1, new Gauge[Long] {
            override def getValue: Long = metric._2
          })
        case ex: Exception =>
          log.error(ex.toString)
      }
    })
  }
}
