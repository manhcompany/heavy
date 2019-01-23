package org.apache.spark.heavy.metrics

import java.util
import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import com.heavy.core.utils.Logging
import com.heavy.monitoring.{PrometheusConfig, SparkApplicationMetricFilter}
import io.prometheus.client.exporter.PushGateway
import io.prometheus.client.CollectorRegistry
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
import scala.util.Try

protected class PrometheusReporter(registry: MetricRegistry, pushGateway: PushGateway) extends
  ScheduledReporter(registry, "prometheus-reporter", new SparkApplicationMetricFilter(), TimeUnit.SECONDS, TimeUnit.MILLISECONDS) with Logging {


  override def report(gauges: util.SortedMap[String, Gauge[_]],
                      counters: util.SortedMap[String, Counter],
                      histograms: util.SortedMap[String, Histogram],
                      meters: util.SortedMap[String, Meter],
                      timers: util.SortedMap[String, Timer]): Unit = {

    val reg = new CollectorRegistry
    if (!gauges.isEmpty) {
      for (entry <- gauges.entrySet) {
        Try {
          io.prometheus.client.Gauge.build()
            .name(entry.getKey.split("\\.").tail.tail.mkString("_"))
            .labelNames(PrometheusConfig.getLabelNames:_*)
            .help("Metrics of QueryExecution")
            .register(reg)
            .labels(PrometheusConfig.getLabels:_*)
            .set(entry.getValue.getValue.toString.toDouble)
        }.recover{ case exp: Throwable => log.info(exp.getMessage) }
      }
    }
    pushGateway.pushAdd(reg, SparkContext.getOrCreate().appName)
  }
}
