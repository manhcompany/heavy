package com.heavy.monitoring

import com.codahale.metrics.{Metric, MetricFilter}
import org.apache.spark.SparkContext

class SparkApplicationMetricFilter extends MetricFilter {
  override def matches(name: String, metric: Metric): Boolean = {
    name.contains(SparkContext.getOrCreate().appName)
  }
}
