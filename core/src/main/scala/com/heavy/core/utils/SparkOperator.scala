package com.heavy.core.utils

import org.apache.spark.sql.DataFrame

abstract class OperatorDecorator(decoratedOperator: Operator[DataFrame]) extends Operator[DataFrame] {
  override def execute(operands: DataFrame*): Option[DataFrame] = decoratedOperator.execute(operands: _*)
}

class ShowDataFrame(decoratedOperator: Operator[DataFrame]) extends OperatorDecorator(decoratedOperator) {
  override def getNumberOperator: Int = decoratedOperator.getNumberOperator

  override def execute(operands: DataFrame*): Option[DataFrame] = {
    val result = decoratedOperator.execute(operands: _*)
    result match {
      case Some(df) => {
        df.show()
        result
      }
      case None => None
    }
  }
}

object SparkOperator {
  class InputOperator(config: OperatorConfig) extends Operand[DataFrame] {
    override def execute(operands: DataFrame*) : Option[DataFrame] = {
      val spark = SparkCommon.getSparkSession
      Some(config.options.get.foldLeft(config.format match {
        case Some(f) => spark.read.format(f)
        case None => spark.read
      })((x, y) => x.option(y.key, y.value)).load(config.path.get))
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
      Some(spark.sql(s"select ${config.select.get.head} from dfl ${config.joinType.getOrElse("inner")} join dfr on ${config.conditions.get}"))
    }
  }

  class SelectOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[DataFrame] = {
      val df = operands.head
      Some(df.selectExpr(config.select.get: _*))
    }
  }

  class UnionOperator(config: OperatorConfig) extends NaryOperator[DataFrame] {
    override def getNumberOperator: Int = {
      config.numberOfInput.getOrElse(2)
    }

    override def execute(operands: DataFrame*): Option[DataFrame] = {
      Some(operands.tail.foldLeft(operands.head)((r, d) => r.union(d)))
    }
  }

  class DeduplicateOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[DataFrame] = {
      Some(config.cols match {
        case Some(cs) => operands.head.dropDuplicates(cs)
        case None => operands.head.dropDuplicates()
      })
    }
  }

  class DropOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[DataFrame] = {
      Some(operands.head.drop(config.cols.get: _*))
    }
  }

  class RenameOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[DataFrame] = {
      Some(config.renamed.get.foldLeft(operands.head)((df, col) => df.withColumnRenamed(col._1, col._2)))
    }
  }

  def apply(config: OperatorConfig): Operator[DataFrame] = {
    new ShowDataFrame(
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
    )
  }
}
