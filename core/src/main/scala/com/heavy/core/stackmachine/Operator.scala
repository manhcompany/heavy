package com.heavy.core.stackmachine

trait Operator[A] {
  type ExecuteType = Seq[Option[A]] => Either[Option[String], Option[List[A]]]
  def getNumberOperator: Int

  val execute : ExecuteType
}

class Operand[A] extends Operator[A] {
  override def getNumberOperator: Int = 0

  override val execute: ExecuteType = _ => Right(None)
}

class UnaryOperator[A] extends Operator[A] {
  override def getNumberOperator: Int = 1

  override val execute: ExecuteType = _ => Right(None)
}

class BinaryOperator[A] extends Operator[A] {
  override def getNumberOperator: Int = 2

  override val execute: ExecuteType = _ => Right(None)
}

class NaryOperator[A] extends Operator[A] {
  override def getNumberOperator: Int = throw new NotImplementedError("Please implement or override getNumberOperator function!")

  override val execute: ExecuteType = _ => Right(None)
}

class StartLabelOperator[A](val label: String) extends Operator[A] {
  override def getNumberOperator: Int = 0

  override val execute: ExecuteType = _ => Right(None)
}

abstract class IfOperator[A](val left: Option[String], val right: Option[String]) extends UnaryOperator[A] {

  def processOperand(operand: A) : Unit

  override val execute: ExecuteType = operands => {
      operands.head match {
        case Some(x) => processOperand(x); Left(right)
        case None => Left(left)
      }
  }
}