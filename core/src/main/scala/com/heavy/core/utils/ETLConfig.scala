package com.heavy.core.utils

import com.typesafe.config.ConfigFactory

case class ETLConfig(operators: List[OperatorConfig])

case class Opt(key: String, value: String)

case class OperatorConfig
(
  name: String,
  path: Option[String],
  format: Option[String],
  options: Option[List[Opt]],
  select: Option[List[String]],
  mode: Option[String],
  partitions: Option[Int],
  partitionBy: Option[List[String]],
  conditions: Option[String],
  joinType: Option[String],
  numberOfInput: Option[Int],
  cols: Option[List[String]],
  renamed: Option[Map[String, String]],
  aliasName: Option[String]
)

