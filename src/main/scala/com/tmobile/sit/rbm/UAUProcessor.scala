package com.tmobile.sit.rbm
import com.tmobile.sit.rbm.pipeline.{CSVReader, CSVWriter, Logger, UAUProcessorCore}
import org.apache.spark.sql.functions.desc

object UAUProcessor extends App with Logger {
  val conf = getConfig()

  implicit val sparkSession = getSparkSession("SIT-RBM",conf.settings.master.get)

  val acc_users_daily = new CSVReader(conf.settings.lookupPath.get + s"acc_users_daily_*.csv", header = true, delimiter = ";")
  val natcoMapping =  new CSVReader(s"${conf.settings.lookupPath.get}NatCoMapping.csv", header = true, delimiter = ";")

  val result = new UAUProcessorCore(acc_users_daily,natcoMapping ).getUAUAggregates

  CSVWriter(result,s"${conf.settings.outputPath.get}f_uau.csv",delimiter = ";").writeData()

  //result.show(false)
}
