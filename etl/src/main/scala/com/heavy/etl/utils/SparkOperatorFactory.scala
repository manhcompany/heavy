package com.heavy.etl.utils

import com.heavy.core.stackmachine.Operator
import org.apache.spark.sql.DataFrame
import java.util.ServiceLoader

import scala.collection.JavaConverters._

trait SparkOperatorFactory {
  def factory(config: OperatorConfig) : Option[Operator[DataFrame]]
}

object SparkOperatorFactory {
  private val factories = ServiceLoader.load(classOf[SparkOperatorFactory])
    .asScala
    .toList
    .map(_.getClass)
    .map(x => x.newInstance())

  def factory(config: OperatorConfig): Option[Operator[DataFrame]] = {
    val operators = factories
      .foldLeft(List[Operator[DataFrame]]())((result, operatorFactory) => operatorFactory.factory(config) match {
        case Some(d) => result :+ d
        case None => result
      })
    if(operators.lengthCompare(1) == 0) {
      Some(operators.head)
    } else {
      throw new Exception(s"We have ${operators.size} object(s) for ${config.name} config name")
    }
  }
}
