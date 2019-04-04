package com.heavy.monitoring

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.QueryExecutionSource
import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.sql.execution.QueryExecution

import scala.collection.mutable

class QueryExecutionListener extends org.apache.spark.sql.util.QueryExecutionListener with Logging {
  private var _currentQueryExecutionId = 0
  val sources = List(new QueryExecutionSource(SparkContext.getOrCreate()))
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = synchronized {
    if(funcName.equals("save")) {
      val metrics = createMetricsMap(qe, Map("durationns" -> durationNs))
      sources.foreach(source => {
        source.register(metrics, _currentQueryExecutionId)
        SparkEnv.get.metricsSystem.registerSource(source)
      })
      _currentQueryExecutionId = _currentQueryExecutionId + 1
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    log.error(exception.toString)
  }

  def createMetricsMap(qe: QueryExecution, additionals: Map[String, Long] = Map[String, Long]()): mutable.Map[String, Long] = {
    val metrics = mutable.Map[String, Long]()
    val queue = mutable.Queue[QueueNode]()
    queue.enqueue(new QueueNode(qe.executedPlan, ""))
    while (queue.nonEmpty) {
      val node = queue.dequeue()
      val plan = node.plan
      plan.children.zipWithIndex.foreach {
        case(child, ext) => queue.enqueue(new QueueNode(child, node.ext + "." + ext))
      }
      plan.metrics.foldLeft(metrics)((ms, m) =>
        ms += (plan.nodeName.replaceAll("\\s", "") + node.ext + "." + m._1 -> m._2.value)
      )
    }
    metrics ++= additionals
  }
}
