package com.tmobile.sit

import com.tmobile.sit.rbm.config.Setup
import org.apache.spark.sql.SparkSession
import com.tmobile.sit.rbm.pipeline.Logger


package object rbm extends  Logger {
  /**
   * Method for returning SparkSession. It sets some basic parameters which can be easily overwritten in spark-submit on production or test cluster.
   * It expects appName as a parameter for better identification of this job on cluster where hundreds of applications may be running.
   * @param sparkAppName - name of the spark application.
   * @return - SparkSession set according the the desired configuration and resources expected to be available.
   */

  def getSparkSession(sparkAppName: String, master: String): SparkSession = {
    SparkSession.builder()
      .master(master)
      .config("spark.executor.instances", "4")
      .config("spark.executor.memory", "4g")
      .config("spark.executor.cores", "1")
      .config("spark.driver.memory", "10g")
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.executor.JavaOptions", "-XX:+UseG1GC")
      .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.app.name", sparkAppName)
      .getOrCreate()
  }

  def getConfig(): Setup = {
    val configFile = if (System.getProperty("os.name").startsWith("Windows")) {
      logger.info("Detected Windows configuration")
      "rbm_config.windows.conf"
    } else if (System.getProperty("os.name").startsWith("Mac")) {
      logger.info("Detected Mac configuration")
      "rbm_config.OM.conf"
    } else {
      logger.info("Detected Mac configuration")
      "rbm_config.linux.conf"
    }

     val conf = new Setup(configFile)

    if (!conf.settings.isAllDefined) {
      logger.error("Application not properly configured!!")
      conf.settings.printMissingFields()
      System.exit(1)
    }

    conf.settings.printAllFields()
    conf
  }
}
