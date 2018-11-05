package com.heavy.etl.utils

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.FlatSpec

class ConfigTest extends FlatSpec {

  behavior of "ConfigTest"

  it should "loadConfig" in {

    val config = Config.loadConfig("etl")
    println(config)
    assertResult("input") {
      config.operators.head.name
    }
    assertResult(1) {
      config.operators.head.options.get.size
    }
    assertResult(",") {
      config.operators.head.options.get.head.value
    }
    assertResult(1) {
      config.operators(1).select.get.size
    }
    assertResult(2) {
      config.operators(2).numberOfInput.get
    }
    assertResult(2) {
      config.operators(3).renamed.get.size
    }
  }

  it should "pass parameter" in {
    val config = Config.loadConfig("etl")
    assertResult("20181028") {
      config.operators.head.name
    }
  }
}
