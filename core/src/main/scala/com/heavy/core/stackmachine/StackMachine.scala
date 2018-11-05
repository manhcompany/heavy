package com.heavy.core.stackmachine

import scala.collection.mutable

object StackMachine {

  def execute[A](operators: Seq[Operator[A]]): Unit = {
    val stack = mutable.Stack[A]()
    operators.foldLeft(stack)((s, o) => {
      val operands = (1 to o.getNumberOperator).toList.map(_ => s.pop())
      o.execute(operands: _*) match {
        case Some(r) => r.foldLeft(s)((a, e) => a.push(e))
        case None => s
      }
    })
  }
}
