package com.heavy.monitoring

class PrometheusLabelConfig(val labelNames: Seq[String], val labels: Seq[String])

object PrometheusLabelConfig {
  var _instance: PrometheusLabelConfig = _

  def init(labelNames: Seq[String], labels: Seq[String]) : Unit = {
    _instance = new PrometheusLabelConfig(labelNames, labels)
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
