package com.tmobile.sit.jobtemplate.config

import com.tmobile.sit.common.config.ServiceConfig

/**
 * Class for reading configuration from environment and mapping values to the case class Settings which holds them.
 * Parameters can be set in a file, as environment variables or JVM parameters.
 * @param configFile - optional parameters config file - default is "job_template.conf"
 */

class Setup(configFile: String = "job_template.conf")  {

  val settings = {
    val serviceConf = new ServiceConfig(Some(configFile))

    Settings(
      appName = Option(serviceConf.envOrElseConfig("configuration.appName.value"))
      , inputPathPeople = Option(serviceConf.envOrElseConfig("configuration.inputPathPeople.value"))
      , inputPathSalaryInfo = Option(serviceConf.envOrElseConfig("configuration.inputPathSalaryInfo.value"))
      , outputPath = Option(serviceConf.envOrElseConfig("configuration.outputPath.value"))
     )
  }
}
