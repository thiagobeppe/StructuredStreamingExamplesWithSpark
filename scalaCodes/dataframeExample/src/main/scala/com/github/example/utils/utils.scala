package com.github.example.utils
import org.apache.log4j.{Level, Logger}

object utils {
  def setupLogging() = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}
