package com.heavy.monitoring

class PrometheusConfig(val labelNames: Seq[String], val labels: Seq[String])

object PrometheusConfig {
  var _instance: PrometheusConfig = _

  def init(labelNames: Seq[String], labels: Seq[String]) : Unit = {
    _instance = new PrometheusConfig(labelNames, labels)
  }

  def getLabels: Seq[String] = {
    if(_instance != null) _instance.labels
    else Seq[String]()
  }

  def getLabelNames: Seq[String] = {
    if(_instance != null) _instance.labelNames
    else Seq[String]()
  }
}
