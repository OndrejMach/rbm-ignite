package com.tmobile.sit

import org.apache.spark.sql.SparkSession

package object rbm {
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
}
