package com.heavy.core.utils

import org.scalatest.FlatSpec

class ConfigTest extends FlatSpec {

  behavior of "ConfigTest"

  it should "loadConfig" in {

    val config = Config.loadConfig("template.conf", "etl")
    println(config)
    assertResult("input") {
      config.operators(0).name
    }
    assertResult(1) {
      config.operators(0).options.get.size
    }
    assertResult(",") {
      config.operators(0).options.get(0).value
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
}
