package com.heavy.etl.utils

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.spark.sql.types.StructField
import org.scalatest.FlatSpec

class ConfigTest extends FlatSpec {

  behavior of "ConfigTest"

//  it should "loadConfig" in {
//
//    val config = Config.loadConfig("etl")
//    println(config)
//    assertResult("input") {
//      config.operators.head.name
//    }
//    assertResult(2) {
//      config.operators.head.options.get.size
//    }
//    assertResult(",") {
//      config.operators.head.options.get.head.value
//    }
//    assertResult(1) {
//      config.operators(1).select.get.size
//    }
//    assertResult(2) {
//      config.operators(2).numberOfInput.get
//    }
//    assertResult(2) {
//      config.operators(3).renamed.get.size
//    }
//  }

  it should "pass parameter" in {
    val config = Config.loadConfig("etl")
    assertResult("20181028") {
      config.operators.head.name
    }
  }

  it should "pass collect" in {
    val mens = List("a", "b")
    val womens = List("c", "d")
    val result = (mens zip womens).collect {
      case name: (String, String) => name._1
    }
    StructField
    print(result)
    assertResult(1)(1)
  }
}
