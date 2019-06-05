package com.heavy.core.stackmachine

import scala.collection.mutable

object CanonicalStackMachine extends AbstractStackMachine {
  override def execute[A](operators: Seq[Operator[A]]): Unit = {
    lazy val branches = getBranches[A](operators)
    val stack = mutable.Stack[Option[A]]()
    executeBranch[A](branches("main"), stack, branches)
  }

  def executeBranch[A](operators: Seq[Operator[A]], stack: mutable.Stack[Option[A]], branches: Map[String, Seq[Operator[A]]]): mutable.Stack[Option[A]] = {
    operators.foldLeft(stack)((s, op) => {
      val operands = (1 to op.getNumberOperator).toList.map(_ => s.pop())
      op.execute(operands) match {
        case Right(odfs) => odfs match {
          case Some(dfs) => if(dfs.nonEmpty) dfs.foldLeft(s)((a, e) => a.push(Some(e)))
            else s.push(None)
          case None => s
        }
        case Left(label) => label match {
          case Some(lableStr) => executeBranch(branches(lableStr), stack, branches)
          case None => s
        }
      }
    })
  }

  def getBranches[A](operators: Seq[Operator[A]]): Map[String, Seq[Operator[A]]] = {
    operators.foldLeft(List[List[Operator[A]]](List[Operator[A]]()))((brs, op) => {
      if (!op.isInstanceOf[StartLabelOperator[A]]) {
        (op::brs.head)::brs.tail
      } else {
        List[Operator[A]](op)::brs
      }
    }).map(x => x.reverse).map(br => {
      br.head match {
        case labelOp: StartLabelOperator[A] =>
          labelOp.label -> br.tail
        case _ =>
          "main" -> br
      }
    }).toMap
  }
}
