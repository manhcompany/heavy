package com.heavy.core.utils

import scala.collection.mutable

object Scheduler {

  def execute[A](operators: Seq[Operator[A]]): Unit = {
    var stack = mutable.Stack[A]()
    operators.foldLeft(stack)((s, o) => {
      val operands = (1 to o.getNumberOperator).toList.map(x => s.pop())
      o.execute(operands: _*) match {
        case Some(r) => r.foldLeft(s)((a, e) => a.push(e))
        case None => s
      }
//      s.push(o.execute(operands: _*) match {
//        case Some(r) => r
//        case None => throw new NotImplementedError("Please implement or override execute function!")
//      })
    })
  }
}
