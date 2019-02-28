package com.heavy.etl.utils

import com.heavy.core.stackmachine.StackMachine
import com.heavy.monitoring.{PrometheusLabelConfig, QueryExecutionListener, QueryExecutionSource}
import org.apache.spark.SparkEnv
import org.apache.spark.heavy.accumulator.AccumulatorCtx
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.ui.heavy.CustomSQLListener
import org.apache.spark.sql.execution.ui.{SQLHistoryListener, SQLListener}
import org.rogach.scallop.ScallopConf
import org.scalatest.FlatSpec

class StackMachineTest extends FlatSpec {



  behavior of "SchedulerTest"

  it should "test config" in {

//    class CustomSparkListener extends SparkListener {
//      override def onJobStart(jobStart: SparkListenerJobStart) {
//        println(s"Job started with ${jobStart.stageInfos.size} stages: $jobStart")
//      }
//
//      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
//        println(s"Stage ${stageCompleted.stageInfo.stageId} completed with ${stageCompleted.stageInfo.numTasks} tasks.")
//      }
//    }

//    PrometheusLabelConfig.init(List("date", "ds"), List("20190111", "voice"))
    val config = Config.loadConfig("etl")
    val operators = config.operators.map(x => SparkOperatorFactory.factory(x).get)
    SparkCommon.getSparkSession("SparkIntercept")
//    val sqlListener = new CustomSQLListener()
//    val sqlHistoryListener = new SQLHistoryListener(SparkCommon.getSparkContext.getConf)
//    SparkCommon.getSparkContext.addSparkListener(sqlListener)
//    SparkCommon.getSparkSession().sessionState.listenerManager.register(new QueryExecutionListener())
    StackMachine.execute[DataFrame](operators)
    StackMachine.execute[DataFrame](operators)
    StackMachine.execute[DataFrame](operators)
//    println("============")
    StackMachine.execute[DataFrame](operators)
    StackMachine.execute[DataFrame](operators)
    StackMachine.execute[DataFrame](operators)
//    val counter = SparkCommon.getSparkContext.longAccumulator("counter")
//    SparkCommon.getSparkContext.parallelize(1 to 9).foreach(x => counter.add(x))
    println("End")
//    while (true) {
//      1 + 1
//    }
//    sqlListener.getCompletedExecutions
//    AccumulatorCtx.get()
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
//    class com.heavy.monitoring.Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//      val apples = opt[Int](required = true)
//      val bananas = opt[Int]()
//      val name = trailArg[String]()
//      verify()
//    }
//  }
}
