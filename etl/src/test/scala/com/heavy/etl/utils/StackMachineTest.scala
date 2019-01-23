package com.heavy.etl.utils

import com.heavy.core.stackmachine.StackMachine
import com.heavy.monitoring.{PrometheusConfig, QueryExecutionListener, QueryExecutionSource}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.ScallopConf
import org.scalatest.FlatSpec

class StackMachineTest extends FlatSpec {

  behavior of "SchedulerTest"

  it should "test config" in {
    PrometheusConfig.init(List("date", "ds"), List("20190111", "voice"))
    val config = Config.loadConfig("etl")
    val operators = config.operators.map(x => SparkOperatorFactory.factory(x).get)
    SparkCommon.getSparkSession("SparkIntercept")
//    SparkCommon.getSparkSession().sessionState.listenerManager.register(new QueryExecutionListener())
    StackMachine.execute[DataFrame](operators)
  }

//  it should "test join config" in {
//    val config = Config.loadConfig("etl")
//    val operators = config.operators.map(x => SparkOperatorFactory.factory(x).get)
//    StackMachine.execute[DataFrame](operators)
//  }
//  it should "test prometheus config" in {
//
//    class PrometheusConfig(val labelNames: Seq[String], val labels: Seq[String])
//
//    object PrometheusConfig {
//      var _instance: PrometheusConfig = _
//
//      def init(labelNames: Seq[String], labels: Seq[String]) : Unit = {
//        _instance = new PrometheusConfig(labelNames, labels)
//      }
//
//      def getLabels: Seq[String] = {
//        _instance.labels
//      }
//
//      def getLabelNames: Seq[String] = {
//        _instance.labelNames
//      }
//    }
//
//    PrometheusConfig.init(List("a", "b", "c"), List("la", "lb", "lc"))
//    println(PrometheusConfig.getLabels)
//  }
//
//  it should "test scallop" in {
//    class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//      val apples = opt[Int](required = true)
//      val bananas = opt[Int]()
//      val name = trailArg[String]()
//      verify()
//    }
//  }
}
