package org.apache.spark.sql.execution.ui

import org.apache.spark.heavy.accumulator.AccumulatorCtx
import org.apache.spark.metrics.source.CustomSQLListenerSource
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.{SparkContext, SparkEnv}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try


class CustomSQLListener extends SQLListener(SparkContext.getOrCreate().getConf) {
  val sources = List(new CustomSQLListenerSource(SparkContext.getOrCreate()))
  val processedExecutions: ListBuffer[Long] = mutable.ListBuffer[Long]()

  def process(): Unit = {
    val qe = super.getCompletedExecutions.filter(e => !processedExecutions.contains(e.executionId) && e.description.contains("save"))
    qe.foreach(q => {
      val edges = q.physicalPlanGraph.edges
      val nodes = q.physicalPlanGraph.allNodes.filter(n => !n.isInstanceOf[SparkPlanGraphCluster])
      val (treeNodes, nodesMap) = nodes.foldLeft((List[TreeNode](), Map[Long, TreeNode]()))((tns, node) => {
        val treeNode = new TreeNode(node)
        (tns._1 ::: List(treeNode), tns._2 + (treeNode.node.id -> treeNode))
      })
      edges.foreach(e => {
        nodesMap(e.toId).addChilds(Seq(nodesMap(e.fromId)))
      })

      val leafs = getLeafs(treeNodes)
      val root = getRoot(treeNodes)

      val m = generateMetrics(root, leafs, q)
      registerSource(m)
      processedExecutions += q.executionId
    })
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit =  {
    synchronized { super.onOtherEvent(event) }
    if (event.isInstanceOf[SparkListenerSQLExecutionEnd]) {
        synchronized { process() }
    }
  }

  def generateMetrics(root: TreeNode, leafs: Seq[TreeNode], q: SQLExecutionUIData): mutable.Map[String, Long] = {
    val metrics: mutable.Map[String, Long] = scala.collection.mutable.Map[String, Long]()
    getNumberOfOutputRows(root).foreach(metrics.update(s"execution_${q.executionId}_number_of_output_rows", _))
    leafs.zipWithIndex.foreach { case(l, i) =>
      metrics.update(s"execution_${q.executionId}_number_of_input_rows_${i.toString}", AccumulatorCtx.get(l.node.metrics.filter(m => m.name.contains("number of output rows")).head.accumulatorId).get.value.toString.toLong)
    }
    metrics
  }

  def registerSource(metrics: scala.collection.mutable.Map[String, Long]): Unit = {
    sources.foreach(source => {
      source.register(metrics)
      SparkEnv.get.metricsSystem.removeSource(source)
      SparkEnv.get.metricsSystem.registerSource(source)
    })
  }

  def getLeafs(tree: Seq[TreeNode]): List[TreeNode] = {
    tree.filter(n => !n.containsChilds).toList
  }

  def getRoot(tree: Seq[TreeNode]): TreeNode = {
    val colors = tree.foldLeft(List[Long]())((nodeColors, node) =>
        node.childs.foldLeft(nodeColors)((x, y) => x ::: List(y.node.id)))
    tree.filter(n => !colors.contains(n.node.id)).head
  }

  def getNumberOfOutputRows(root: TreeNode): Option[Long] = {
    val queue = mutable.Queue[TreeNode]()
    var whileFlag = true
    var accumulatorId: Option[Long] = None
    queue.enqueue(root)
    while (queue.nonEmpty && whileFlag) {
      val node = queue.dequeue()
      if (node.node.metrics.exists(m => m.name.contains("number of output rows"))) {
        whileFlag = false
        accumulatorId = node.node.metrics.find(m => m.name.contains("number of output rows")).map(_.accumulatorId)
      } else {
        node.childs.foreach(c => queue.enqueue(c))
      }
    }
    for {
      acc <- accumulatorId
      accOpt <- AccumulatorCtx.get(acc)
      accStr = accOpt.value.toString
      ret <- Try(accStr.toLong).toOption
    } yield ret
  }
}
