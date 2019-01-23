package org.apache.spark.heavy.metrics

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import io.prometheus.client.exporter.PushGateway

class PrometheusSink(val property: Properties, val registry: MetricRegistry, val securityMgr: org.apache.spark.SecurityManager)
  extends Sink {

  val ADDRESS = "pushgateway-address"
  val pushGateway = new PushGateway(property.getProperty(ADDRESS))
  val reporter: PrometheusReporter = new PrometheusReporter(registry, pushGateway)

  override def start(): Unit = {
    reporter.start(10, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    reporter.stop()
  }

  override def report(): Unit = {
    reporter.report()
  }
}
