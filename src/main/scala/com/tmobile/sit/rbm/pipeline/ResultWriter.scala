package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.writers.CSVWriter
import org.apache.spark.sql.SparkSession

trait Writer {
  def write(output: OutputData): Unit
}

class ResultWriter(resultPaths: ResultPaths) (implicit sparkSession: SparkSession) extends Writer {
  override def write(outputData: OutputData) =
  {
    /* Write output files*/
    CSVWriter(outputData.NatCoMapping, resultPaths.outputPath+"NatCoMapping.csv", delimiter = ";").writeData()
    CSVWriter(outputData.ContentMapping, resultPaths.outputPath+"ContentMapping.csv", delimiter = ";").writeData()
    CSVWriter(outputData.AgentMapping, resultPaths.outputPath+"AgentMapping.csv", delimiter = ";").writeData()
  }
}