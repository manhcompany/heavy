package com.heavy.etl.utils

import java.io.File

import com.typesafe.config.ConfigFactory

object Config {
  def loadConfig(path: String, namespace: String): ETLConfig = {
    val config = ConfigFactory.parseFile(new File(path)).getConfig(namespace)
    pureconfig.loadConfigOrThrow[ETLConfig](config)
  }
}