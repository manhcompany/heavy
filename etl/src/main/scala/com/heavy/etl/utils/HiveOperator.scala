package com.heavy.etl.utils

import com.heavy.core.stackmachine.{Operator, UnaryOperator}
import org.apache.spark.sql.DataFrame

import scala.util.Try

class HiveOperator extends SparkOperatorFactory {

  class SaveAsTableOperator(config: OperatorConfig) extends UnaryOperator[DataFrame] {
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
      writerFinal.saveAsTable(config.path.get)
      None
    }
  }

  override def factory(config: OperatorConfig): Option[Operator[DataFrame]] = {
    Try(Some(new ShowDataFrame(
          config.name match {
            case "save-as-table" => new SaveAsTableOperator(config)
          }))
    ).map(d => d).recover{ case exp: Throwable => None }.get
  }
}
