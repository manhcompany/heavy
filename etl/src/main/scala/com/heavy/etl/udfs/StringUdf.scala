package com.heavy.etl.udfs

import com.heavy.core.utils.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

class StringUdf extends SparkUdfInterceptor with Logging{

  override def intercept(spark: SparkSession): Unit = {
    log.info("StringUdf intercept")
    lazy val stringUdfFuncs = Map(
      "uppercase" -> ((x: String) => x.toUpperCase)
    )

    stringUdfFuncs.foreach {
      case (name, func) => spark.udf.register(name, func)
    }
  }
}
