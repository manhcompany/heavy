package com.heavy.core.utils

import org.apache.spark.sql.DataFrame

object SparkOperator {
  class InputOperator(config: OperatorConfig) extends Operand[DataFrame] {
    override def execute(operands: DataFrame*) : Option[DataFrame] = {
      val spark = SparkCommon.getSparkSession
      val result = config.options.get.foldLeft(config.format match {
        case Some(f) => spark.read.format(f)
        case None => spark.read
      })((x, y) => x.option(y.key, y.value)).load(config.path.get)
      result.show()
      Some(result)
    }
  }

  class OutputOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[DataFrame] = {
      val writePartitions = config.partitions match {
        case Some(nop) => operands.head.repartition(nop)
        case None => operands.head
      }

      val writerPartitionBy = config.partitionBy match {
        case Some(field) => writePartitions.write.partitionBy(field: _*)
        case None => writePartitions.write
      }

      val writerMode = config.mode match {
        case Some(m) => writerPartitionBy.mode(m)
        case None => writerPartitionBy
      }

      val writerFinal = config.options match {
        case Some(opts) => opts.foldLeft(writerMode)((writer, opt) => writer.option(opt.key, opt.value))
        case None => writerMode
      }
      writerFinal.save(config.path.get)
      Some(operands.head)
    }
  }

  class JoinOperator(config: OperatorConfig) extends BinaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[DataFrame] = {
      val dfR = operands(1)
      val dfL = operands(0)
      dfL.createOrReplaceTempView("dfl")
      dfR.createOrReplaceTempView("dfr")
      val spark = SparkCommon.getSparkSession
      val result = spark.sql(s"select ${config.select.get.head} from dfl ${config.joinType.getOrElse("inner")} join dfr on ${config.conditions.get}")
      result.show
      Some(result)
    }
  }

  class SelectOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[DataFrame] = {
      val df = operands.head
      val result = df.selectExpr(config.select.get: _*)
      result.show()
      Some(result)
    }
  }

  class UnionOperator(config: OperatorConfig) extends NaryOperator[DataFrame] {
    override def getNumberOperator: Int = {
      config.numberOfInput.getOrElse(2)
    }

    override def execute(operands: DataFrame*): Option[DataFrame] = {
      val result = operands.tail.foldLeft(operands.head)((r, d) => r.union(d))
      result.show()
      Some(result)
    }
  }

  class DeduplicateOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[DataFrame] = {
      val result = config.cols match {
        case Some(cs) => operands.head.dropDuplicates(cs)
        case None => operands.head.dropDuplicates()
      }
      result.show()
      Some(result)
    }
  }

  class DropOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[DataFrame] = {
      val result = operands.head.drop(config.cols.get: _*)
      result.show()
      Some(result)
    }
  }

  class RenameOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[DataFrame] = {
      val result = config.renamed.get.foldLeft(operands.head)((df, col) => df.withColumnRenamed(col._1, col._2))
      result.show()
      Some(result)
    }
  }

  def apply(config: OperatorConfig): Operator[DataFrame] = {
    config.name match {
      case "input" => new InputOperator(config = config)
      case "output" => new OutputOperator(config=config)
      case "select" => new SelectOperator(config=config)
      case "join" => new JoinOperator(config=config)
      case "union" => new UnionOperator(config=config)
      case "dedup" => new DeduplicateOperator(config=config)
      case "drop" => new DropOperator(config=config)
      case "rename" => new RenameOperator(config=config)
    }
  }
}
