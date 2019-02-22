package org.apache.spark.sql.execution.ui.heavy

import org.apache.spark.heavy.accumulator.AccumulatorCtx
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.execution.ui.{SQLExecutionUIData, SQLListener, SparkListenerSQLExecutionEnd, SparkPlanGraphCluster}
import org.apache.spark.{SparkContext, SparkEnv}

import scala.collection.mutable


class CustomSQLListener extends SQLListener(SparkContext.getOrCreate().getConf) {
  val sources = List(new CustomSQLListenerSource(SparkContext.getOrCreate()))

  def process(): Unit = {
    val qe = super.getCompletedExecutions.filter(e => e.description.contains("save"))
    qe.foreach(q => {
      val nodesMap = scala.collection.mutable.Map[Long, TreeNode]()
      var treeNodes = scala.collection.mutable.ListBuffer[TreeNode]()
      val edges = q.physicalPlanGraph.edges
      val nodes = q.physicalPlanGraph.allNodes.filter(n => !n.isInstanceOf[SparkPlanGraphCluster])
      nodes.foreach(node => {
        val treeNode = new TreeNode(node)
        nodesMap.update(treeNode.node.id, treeNode)
        treeNodes += treeNode
      })
      edges.foreach(e => {
        nodesMap(e.toId).addChilds(Seq(nodesMap(e.fromId)))
      })

      val leafs = getLeafs(treeNodes)
      val root = getRoot(treeNodes)

      val metrics = generateMetrics(root, leafs, q)
      registerSource(metrics)
    })
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = synchronized {
    super.onOtherEvent(event)
    if (event.isInstanceOf[SparkListenerSQLExecutionEnd]) process()
  }

  def generateMetrics(root: TreeNode, leafs: Seq[TreeNode], q: SQLExecutionUIData): mutable.Map[String, Long] = {
    val metrics = scala.collection.mutable.Map[String, Long]()
    metrics.update("execution_" + q.executionId + "_number_of_output_rows", getNumberOfOutputRows(root))
    var i: Int = 0
    leafs.foreach(l => {
      metrics.update("execution_" + q.executionId + "_number_of_input_rows_" + i.toString, AccumulatorCtx.get(l.node.metrics.filter(m => m.name.contains("number of output rows")).head.accumulatorId).get.value.toString.toLong)
      i = i + 1
    })
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
    val colors = tree.foldLeft(scala.collection.mutable.ListBuffer[Long]())((nodeColors, node) =>
        node.childs.foldLeft(nodeColors)((x, y) => x += y.node.id))
    tree.filter(n => !colors.contains(n.node.id)).head
  }

  def getNumberOfOutputRows(root: TreeNode): Long = {
    val queue = mutable.Queue[TreeNode]()
    var whileFlag = true
    var accumulatorId: Long = 0
    queue.enqueue(root)
    while (queue.nonEmpty && whileFlag) {
      val node = queue.dequeue()
      if (node.node.metrics.exists(m => m.name.contains("number of output rows"))) {
        whileFlag = false
        accumulatorId = node.node.metrics.filter(m => m.name.contains("number of output rows")).head.accumulatorId
      } else {
        node.childs.foreach(c => queue.enqueue(c))
      }
    }
    AccumulatorCtx.get(accumulatorId).get.value.toString.toLong
  }
}
