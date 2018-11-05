package com.heavy.etl.utils

import java.io.File

import com.typesafe.config.ConfigFactory

import scala.io.Source

object Config {
  def loadConfig(namespace: String): ETLConfig = {
    val config = ConfigFactory.load()
    pureconfig.loadConfigOrThrow[ETLConfig](config, namespace)
  }
}