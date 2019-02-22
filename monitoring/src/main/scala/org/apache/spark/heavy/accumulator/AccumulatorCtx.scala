package org.apache.spark.heavy.accumulator

import org.apache.spark.util.{AccumulatorContext, AccumulatorV2}

object AccumulatorCtx {
  def get(id: Long): Option[AccumulatorV2[_, _]] = {
    AccumulatorContext.get(id = id)
  }
}
