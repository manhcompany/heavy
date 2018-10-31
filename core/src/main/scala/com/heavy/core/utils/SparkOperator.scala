package com.heavy.core.utils

import org.apache.spark.sql.DataFrame

abstract class OperatorDecorator(decoratedOperator: Operator[DataFrame]) extends Operator[DataFrame] {
  override def execute(operands: DataFrame*): Option[List[DataFrame]] = decoratedOperator.execute(operands: _*)
}

class ShowDataFrame(decoratedOperator: Operator[DataFrame]) extends OperatorDecorator(decoratedOperator) {
  override def getNumberOperator: Int = decoratedOperator.getNumberOperator

  override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
    val result = decoratedOperator.execute(operands: _*)
    result match {
      case Some(dfs) =>
        dfs.foreach(df => df.show())
        result
      case None => None
    }
  }
}

object SparkOperator {

  var aliases: Map[String, DataFrame] = Map()

  class InputOperator(config: OperatorConfig) extends Operand[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      val spark = SparkCommon.getSparkSession
      Some(List(config.options.get.foldLeft(config.format match {
        case Some(f) => spark.read.format(f)
        case None => spark.read
      })((x, y) => x.option(y.key, y.value)).load(config.path.get)))
    }
  }

  class OutputOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
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
      Some(List(operands.head))
    }
  }

  class JoinOperator(config: OperatorConfig) extends BinaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      val dfR = operands(1)
      val dfL = operands(0)
      dfL.createOrReplaceTempView("dfl")
      dfR.createOrReplaceTempView("dfr")
      val spark = SparkCommon.getSparkSession
      Some(List(spark.sql(s"select ${config.select.get.head} from dfl ${config.joinType.getOrElse("inner")} join dfr on ${config.conditions.get}")))
    }
  }

  class SelectOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      val df = operands.head
      Some(List(df.selectExpr(config.select.get: _*)))
    }
  }

  class UnionOperator(config: OperatorConfig) extends NaryOperator[DataFrame] {
    override def getNumberOperator: Int = {
      config.numberOfInput.getOrElse(2)
    }

    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      Some(List(operands.tail.foldLeft(operands.head)((r, d) => r.union(d))))
    }
  }

  class DeduplicateOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      config.cols match {
        case Some(cs) => Some(List(operands.head.dropDuplicates(cs)))
        case None => Some(List(operands.head.dropDuplicates()))
      }
    }
  }

  class DropOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      Some(List(operands.head.drop(config.cols.get: _*)))
    }
  }

  class RenameOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      Some(List(config.renamed.get.foldLeft(operands.head)((df, col) => df.withColumnRenamed(col._1, col._2))))
    }
  }

  class FilterOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      Some(List(operands.head.filter(config.conditions.get)))
    }
  }

  class AliasOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      aliases += (config.aliasName.get -> operands.head)
      None
    }
  }

  class LoadAliasOperator(config: OperatorConfig) extends Operand[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      Some(List(aliases(config.aliasName.get)))
    }
  }

  def apply(config: OperatorConfig): Operator[DataFrame] = {
    new ShowDataFrame(
      config.name match {
        case "input" => new InputOperator(config)
        case "output" => new OutputOperator(config)
        case "select" => new SelectOperator(config)
        case "join" => new JoinOperator(config)
        case "union" => new UnionOperator(config)
        case "dedup" => new DeduplicateOperator(config)
        case "drop" => new DropOperator(config)
        case "rename" => new RenameOperator(config)
        case "alias" => new AliasOperator(config)
        case "load-alias" => new LoadAliasOperator(config)
      }
    )
  }
}
