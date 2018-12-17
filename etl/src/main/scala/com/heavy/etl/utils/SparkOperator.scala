package com.heavy.etl.utils

import com.heavy.core.stackmachine._
import com.heavy.core.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

abstract class OperatorDecorator(decoratedOperator: Operator[DataFrame]) extends Operator[DataFrame] {
  override def execute(operands: DataFrame*): Option[List[DataFrame]] = decoratedOperator.execute(operands: _*)
}

class ShowDataFrame(decoratedOperator: Operator[DataFrame]) extends OperatorDecorator(decoratedOperator) with Logging {
  override def getNumberOperator: Int = decoratedOperator.getNumberOperator

  override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
    val result = decoratedOperator.execute(operands: _*)
    result match {
      case Some(dfs) =>
        log.info("Schema")
        dfs.foreach(df => log.info(df.show.toString))
        result
      case None => None
    }
  }
}

class SparkOperator extends SparkOperatorFactory {

  var aliases: Map[String, DataFrame] = Map()

  class InputOperator(config: OperatorConfig) extends Operand[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      val spark = SparkCommon.getSparkSession
      val readerFormat = config.format match {
        case Some(f) => spark.read.format(f)
        case None => spark.read
      }

      val readerOpts = config.options match {
        case Some(opt) => opt.foldLeft(readerFormat)((r, o) => r.option(o.key, o.value))
        case None => readerFormat
      }
      Some(List(readerOpts.load(config.path.get)))
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

      val writeFormat = config.format match {
        case Some(f) => writerMode.format(f)
        case None => writerMode
      }

      val writerFinal = config.options match {
        case Some(opts) => opts.foldLeft(writeFormat)((writer, opt) => writer.option(opt.key, opt.value))
        case None => writeFormat
      }
      writerFinal.save(config.path.get)
      None
    }
  }

  class JoinOperator(config: OperatorConfig) extends BinaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      val dfL = operands(1)
      val dfR = operands(0)
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

  class IncrementalOperator(config: OperatorConfig) extends BinaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      val df = operands(1)
      val maxCurrentId = operands(0).collect().filter(!_.isNullAt(0)).map(_.getLong(0)).headOption.getOrElse(0L) + 1L
      Some(List(df.withColumn(config.cols.get.head, monotonically_increasing_id() + maxCurrentId)))
    }
  }

  class ExceptOperator(config: OperatorConfig) extends BinaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      val dff = operands(1)
      val dfs = operands(0)
      Some(List(dff.except(dfs)))
    }
  }

  class SqlOperator(config: OperatorConfig) extends Operand[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      val spark = SparkCommon.getSparkSession
      Some(List(spark.sql(config.query.get)))
    }
  }

  class RegisterTempView(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      operands.head.createOrReplaceTempView(config.viewName.get)
      None
    }
  }

  class Repartition(config: OperatorConfig) extends UnaryOperator[DataFrame] {
    override def execute(operands: DataFrame*): Option[List[DataFrame]] = {
      Option(List(operands.head.repartition(config.partitions.get)))
    }
  }

  override def factory(config: OperatorConfig): Option[Operator[DataFrame]] = {
    Option(new ShowDataFrame(
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
        case "incremental" => new IncrementalOperator(config)
        case "except" => new ExceptOperator(config)
        case "sql" => new SqlOperator(config)
        case "view" => new RegisterTempView(config)
        case "repartition" => new Repartition(config)
      }
    ))
  }
}
