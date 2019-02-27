package org.apache.spark.sql.execution.ui.heavy

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode

import scala.collection.mutable.ListBuffer

class TreeNode(val node: SparkPlanGraphNode) {
  var childs = new ListBuffer[TreeNode]()

  def addChilds(nodes: Seq[TreeNode]): Unit = {
    childs ++= nodes
  }

  def getChilds: List[TreeNode] = {
    childs.toList
  }

  def containsChilds: Boolean = {
    childs.nonEmpty
  }
}