package com.tmobile.sit.rbm.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Properties

/**
 * Helper class for retrieving parameters from enviroment (file, variables, ...). The most important parts are:
 * 1) reading parameter values from environment
 * 2) type casting
 * *
 *
 * @param filename - path to the configuration file
 *                 *
 * @author Ondrej Machacek
 */

class ServiceConfig(filename: Option[String] = None) {
  val ARRAY_DELIMITER: String = ","

  val config: Config = filename.fold(ifEmpty = ConfigFactory.load())(file => ConfigFactory.load(file))

  /**
   * getting parameter value from environment - checks environment variables, JVM parameters and configuration file
   *
   * @param name parameter name
   */
  def envOrElseConfig(name: String): String = {
    config.resolve()
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }

  /**
   * getting parameter value from environment as Int if possible - checks environment variables, JVM parameters and configuration file
   *
   * @param name parameter name
   */
  def getInt(name: String): Option[Int] = {
    try {
      val i = envOrElseConfig(name).toInt
      if (i >= 0) {
        Some(i)
      } else {
        None
      }
    } catch {
      case e: Exception => None
    }
  }

  /**
   * getting parameter value from environment as Long if possible - checks environment variables, JVM parameters and configuration file
   *
   * @param name parameter name
   */
  def getLong(name: String): Option[Long] = {
    try {
      Some(envOrElseConfig(name).toLong)
    } catch {
      case e: NumberFormatException => None
    }
  }

  /**
   * getting parameter value from environment as Double if possible - checks environment variables, JVM parameters and configuration file
   *
   * @param name parameter name
   */
  def getDouble(name: String): Option[Double] = {
    try {
      Some(envOrElseConfig(name).toDouble)
    } catch {
      case e: NumberFormatException => None
    }
  }

  /**
   * getting parameter value from environment as String if possible - checks environment variables, JVM parameters and configuration file
   *
   * @param name parameter name
   */
  def getString(name: String): Option[String] = {
    try {
      Some(envOrElseConfig(name))
    } catch {
      case e: Exception => None
    }
  }

  /**
   * getting parameter value from environment as array - checks environment variables, JVM parameters and configuration file.
   * It expects array is specified as a string with ',' as the separater.
   *
   * @param name parameter name
   */
  def getIntArray(name: String): Option[Array[Int]] = {
    try {
      val s = Some(envOrElseConfig(name)).get
      Some(s.split(ARRAY_DELIMITER).map(_.toInt))
    } catch {
      case e: Exception => None
    }
  }
}