package org.apache.spark.metrics.source

import com.codahale.metrics._
import org.apache.spark.SparkContext

import scala.collection.mutable

class QueryExecutionSource(sc: SparkContext) extends Source {
  override val sourceName: String = sc.appName

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  def register(metrics: mutable.Map[String, Long], executionId: Int): Unit = {
    metrics.foreach(metric => {
      metricRegistry.register(s"query_execution_${executionId}_${metric._1}", new Gauge[Long] {
        override def getValue: Long = metric._2
      })
    })
  }
}
