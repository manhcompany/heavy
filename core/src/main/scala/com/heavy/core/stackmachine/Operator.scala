package com.heavy.core.stackmachine

trait Operator[A] {
  def getNumberOperator: Int

  def execute(operands: Option[A]*): Either[String, Option[List[A]]]
}

class Operand[A] extends Operator[A] {
  override def getNumberOperator: Int = 0

  override def execute(operands: Option[A]*): Either[String, Option[List[A]]] = Right(None)
}

class UnaryOperator[A] extends Operator[A] {
  override def getNumberOperator: Int = 1

  override def execute(operands: Option[A]*): Either[String, Option[List[A]]] = Right(None)
}

class BinaryOperator[A] extends Operator[A] {
  override def getNumberOperator: Int = 2

  override def execute(operands: Option[A]*): Either[String, Option[List[A]]] = Right(None)
}

class NaryOperator[A] extends Operator[A] {
  override def getNumberOperator: Int = throw new NotImplementedError("Please implement or override getNumberOperator function!")

  override def execute(operands: Option[A]*): Either[String, Option[List[A]]] = Right(None)
}

class StartLabelOperator[A](val label: String) extends Operator[A] {
  override def getNumberOperator: Int = 0

  override def execute(operands: Option[A]*): Either[String, Option[List[A]]] = Right(None)
}

abstract class IfOperator[A](val left: String, val right: String) extends UnaryOperator[A] {

  def processOperand(operand: A) : Unit

  override def execute(operands: Option[A]*): Either[String, Option[List[A]]] = {
    operands.head match {
      case Some(x) => processOperand(x); Left(right)
      case None => Left(left)
    }
  }
}