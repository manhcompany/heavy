package com.heavy.etl.utils
import com.heavy.core.stackmachine.{IfOperator, Operator, StartLabelOperator}
import org.apache.spark.sql.DataFrame

import scala.util.Try

class BranchingOperator extends SparkOperatorFactory {
  override def factory(config: OperatorConfig): Option[Operator[DataFrame]] = {
    Try(Some(
      config.name match {
        case "if" => new IfOperator[DataFrame](config.left, config.right) {
          override def processOperand(operand: DataFrame): Unit = {
            operand.show()
          }
        }
        case "label" => new StartLabelOperator[DataFrame](config.label.get)
      })
    ).map(d => d).recover { case _: Throwable => None }.get
  }
}
