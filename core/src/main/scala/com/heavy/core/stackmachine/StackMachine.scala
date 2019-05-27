package com.heavy.core.stackmachine

import scala.collection.mutable

object StackMachine extends AbstractStackMachine {
  override def execute[A](operators: Seq[Operator[A]]): Unit = {
    val stack = mutable.Stack[Option[A]]()
    operators.foldLeft(stack)((s, o) => {
      val operands = (1 to o.getNumberOperator).toList.map(_ => s.pop())
      o.execute(operands: _*) match {
        case Right(odfs) => odfs match {
          case Some(r) => r.foldLeft(s)((a, e) => a.push(Some(e)))
          case None => s
        }
      }
    })
  }
}
