package com.tmobile.sit.rbm.config

/**
 * Class for reading configuration from environment and mapping values to the case class Settings which holds them.
 * Parameters can be set in a file, as environment variables or JVM parameters.
 * @param configFile - optional parameters config file - default is "rbm_config.windows.conf"
 */

class Setup(configFile: String = "rbm_config.windows.conf")  {

  val settings: Settings = {
    val serviceConf = new ServiceConfig(Some(configFile))

    Settings(
      appName = Option(serviceConf.envOrElseConfig("configuration.appName.value"))
      , inputPath = Option(serviceConf.envOrElseConfig("configuration.inputPath.value"))
      , lookupPath = Option(serviceConf.envOrElseConfig("configuration.lookupPath.value"))
      , outputPath = Option(serviceConf.envOrElseConfig("configuration.outputPath.value"))
     )
  }
}
