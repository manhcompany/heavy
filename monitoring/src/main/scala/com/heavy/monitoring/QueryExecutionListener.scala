package com.heavy.monitoring

import com.heavy.core.utils.Logging
import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.sql.execution.QueryExecution

import scala.collection.mutable

class QueryExecutionListener extends org.apache.spark.sql.util.QueryExecutionListener with Logging {
  val sources = List(new QueryExecutionSource(SparkContext.getOrCreate()))
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if(funcName.equals("save")) {
      log.info("Duration = " + durationNs.toString + " ns")
      val metrics = createMetricsMap(qe)
      metrics += ("durationns" -> durationNs)
      sources.foreach(source => {
        source.register(metrics)
        SparkEnv.get.metricsSystem.registerSource(source)
      })
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    log.error(exception.toString)
  }

  def createMetricsMap(qe: QueryExecution): mutable.Map[String, Long] = {
    val metrics = mutable.Map[String, Long]()
    val queue = mutable.Queue[QueueNode]()
    queue.enqueue(new QueueNode(qe.executedPlan, ""))
    while (queue.nonEmpty) {
      val node = queue.dequeue()
      val plan = node.plan
      var ext: Int = 0
      plan.children.foreach(child => {
        queue.enqueue(new QueueNode(child, node.ext + "." + ext))
        ext = ext + 1
      })
      node.plan.metrics.foreach(m => {
        metrics += (node.plan.nodeName.replaceAll("\\s", "") + node.ext + "." + m._1 -> m._2.value)
      })
    }
    metrics
  }
}
