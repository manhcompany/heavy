package org.apache.spark.heavy.metrics

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.heavy.monitoring.PrometheusLabelConfig
import io.prometheus.client.exporter.PushGateway

class PushGatewaySink(val property: Properties, val registry: MetricRegistry, val securityMgr: org.apache.spark.SecurityManager)
  extends Sink {


  val ADDRESS = "pushgateway-address"
  val PERIOD = "period"
  val PERIOD_UNIT = "unit"
  val pushGateway = new PushGateway(property.getProperty(ADDRESS))
  val reporter: PushGatewayReporter = new PushGatewayReporter(registry, pushGateway)

  override def start(): Unit = {
    PrometheusLabelConfig.init(System.getProperty("prometheus.label.names").split(","), System.getProperty("prometheus.label.values").split(","))
    reporter.start(property.getProperty(PERIOD).toInt, Option(property.getProperty(PERIOD_UNIT))
      .map(s => TimeUnit.valueOf(s.toUpperCase)).getOrElse(TimeUnit.SECONDS))
  }

  override def stop(): Unit = {
    reporter.stop()
  }

  override def report(): Unit = {
    reporter.report()
  }
}
