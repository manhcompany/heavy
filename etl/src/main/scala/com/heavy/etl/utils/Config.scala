package com.heavy.etl.utils

import com.typesafe.config.ConfigFactory

object Config {
  def loadConfig(namespace: String): ETLConfig = {
    val config = ConfigFactory.load()
    pureconfig.loadConfigOrThrow[ETLConfig](config, namespace)
  }
}