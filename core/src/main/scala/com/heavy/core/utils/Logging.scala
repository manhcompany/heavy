package com.heavy.core.utils

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  private def loggerClass: Class[_] = {
    var clazz: Class[_] = getClass
    val name = clazz.getName
    while (name.contains("$") && !name.endsWith("$") && clazz != null) {
      clazz = clazz.getSuperclass
    }

    if (clazz == null) {
      getClass.asInstanceOf[Class[_]]
    } else {
      clazz
    }
  }

  protected lazy val log: Logger = LoggerFactory.getLogger(loggerClass)
}