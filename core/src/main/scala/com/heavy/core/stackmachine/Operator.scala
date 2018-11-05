package com.heavy.core.stackmachine

trait Operator[A] {
  def getNumberOperator: Int

  def execute(operands: A*): Option[List[A]]
}

class Operand[A] extends Operator[A] {
  override def getNumberOperator: Int = 0

  override def execute(operands: A*): Option[List[A]] = None
}

class UnaryOperator[A] extends Operator[A] {
  override def getNumberOperator: Int = 1

  override def execute(operands: A*): Option[List[A]] = None
}

class BinaryOperator[A] extends Operator[A] {
  override def getNumberOperator: Int = 2

  override def execute(operands: A*): Option[List[A]] = None
}

class NaryOperator[A] extends Operator[A] {
  override def getNumberOperator: Int = throw new NotImplementedError("Please implement or override getNumberOperator function!")

  override def execute(operands: A*): Option[List[A]] = None
}