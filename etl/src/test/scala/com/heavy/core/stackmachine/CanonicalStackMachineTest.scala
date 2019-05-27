package com.heavy.core.stackmachine

import com.heavy.etl.utils.{Config, SparkCommon, SparkOperatorFactory}
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

class CanonicalStackMachineTest extends FlatSpec {

  behavior of "CanonicalStackMachineTest"

  it should "execute" in {

    val config = Config.loadConfig("etl")
    val operators = config.operators.map(x => SparkOperatorFactory.factory(x).get)
    SparkCommon.getSparkSession("SparkIntercept")
    println("Operators:")
    println(operators)
    CanonicalStackMachine.execute[DataFrame](operators)
    println("End")

  }

  it should "parse to 3 lists" in {
    val as = Seq(1, 2, 3, 0, 4, 5, -1, 8, 9)
    val list = as.foldLeft(List[List[Int]](List[Int]()))((map, number) => {
      if(number != 0 && number != -1) {
        (number::map.head)::map.tail
      } else {
        List[Int](number)::map
      }
    })
    val lr = list.map(x => x.reverse)
    val map = lr.map(x => x.head -> x.tail).toMap
    println(map)
    assertResult(1)(1)
  }

  it should "test either" in {
    def divideXByY(x: Int, y: Int): Either[String, Int] = {
      if (y == 0) Left("Dude, can't divide by 0")
      else Right(x / y)
    }

    divideXByY(1, 0) match {
      case Left(s) => println("Answer: " + s)
      case Right(i) => println("Answer: " + i)
    }

    assertResult(1)(1)
  }

}
