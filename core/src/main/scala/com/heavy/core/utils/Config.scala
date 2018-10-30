package com.heavy.core.utils

import com.typesafe.config.ConfigFactory

object Config {
  def loadConfig(path: String, namespace: String): ETLConfig = {
    val config = ConfigFactory.load(path).getConfig(namespace)
    pureconfig.loadConfigOrThrow[ETLConfig](config)
  }
}