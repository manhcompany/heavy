package com.heavy.core.stackmachine

trait AbstractStackMachine {
  def execute[A](operators: Seq[Operator[A]]): Unit
}
