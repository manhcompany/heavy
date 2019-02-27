package org.apache.spark.sql.execution.ui.heavy

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.SparkContext
import org.apache.spark.heavy.metrics.Source

import scala.collection.mutable
import scala.util.Try

class CustomSQLListenerSource(sc: SparkContext) extends Source {
  override val sourceName: String = sc.appName

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  def register(metrics: mutable.Map[String, Long]): Unit = {
    metrics.foreach(metric => {
      Try(metricRegistry.register(metric._1, new Gauge[Long] {
        override def getValue: Long = metric._2
      })).map(d => d)
        .recover{ case _: IllegalArgumentException =>
          metricRegistry.remove(metric._1)
          metricRegistry.register(metric._1, new Gauge[Long] {
            override def getValue: Long = metric._2
          })
        }
        .get
    })
  }
}
