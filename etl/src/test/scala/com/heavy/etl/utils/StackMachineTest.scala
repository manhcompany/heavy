package com.heavy.etl.utils

import com.heavy.core.stackmachine.StackMachine
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

import scala.collection.mutable

class StackMachineTest extends FlatSpec {
  behavior of "SchedulerTest"
  it should "test config" in {
    val config = Config.loadConfig("etl")
    val operators = config.operators.map(x => SparkOperatorFactory.factory(x).get)
    SparkCommon.getSparkSession("SparkIntercept")
    StackMachine.execute[DataFrame](operators)
    println("End")
  }

  it should "stop" in {
    val stackResult = mutable.Stack[Int]()
    var stackOperator = mutable.Stack[Int]()
    stackOperator.push(1, 2, 3, 4, 5)
    stackOperator.foldLeft(stackResult)((x, y) => {
      stackOperator.pop()
      if (y == 3) stackOperator.push(10)
      println(y)
      x.push(y)
      x
    })

    println(stackResult)
    println(stackOperator)
    assertResult(1)(1)
  }

  it should "print 10" in {
    val stackResult = mutable.Stack[Int]()
    var stackOperator = mutable.Stack[Int]()
    stackOperator.push(1, 2, 3, 4, 5)

    while (stackOperator.nonEmpty) {
      val y = stackOperator.pop()
      if(y == 3) stackOperator.push(10)
      println(y)
      stackResult.push(y)
    }
    println(stackResult)
    println(stackOperator)
    assertResult(1)(1)
  }
}
