package com.tmobile.sit.rbm.pipeline
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Trait for any data reader.
 */

trait Reader extends Logger{
  def read(): DataFrame

  //def readFromPath(path: String): DataFrame
}
