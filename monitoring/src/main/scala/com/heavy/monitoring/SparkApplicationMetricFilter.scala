package com.heavy.monitoring

import com.codahale.metrics.{Metric, MetricFilter}
import com.heavy.core.utils.Logging
import org.apache.spark.SparkContext

class SparkApplicationMetricFilter extends MetricFilter with Logging {
  override def matches(name: String, metric: Metric): Boolean = {
    name.contains(SparkContext.getOrCreate().appName)
  }
}
