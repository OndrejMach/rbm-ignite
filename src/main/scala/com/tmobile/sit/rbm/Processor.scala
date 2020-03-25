package com.tmobile.sit.rbm

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.rbm.config.Setup
import com.tmobile.sit.rbm.pipeline.{CoreLogicWithTransform, InputData, Pipeline, ResultPaths, ResultWriter, Stage}
import org.apache.spark.sql.SparkSession

/**
 * Main object and entry point to the the job processing. It handles configuration and initialised all the processing steps. This design provides
 * better insight into what is actually done with the data and also helps with application testing. It is very easy to test each component and step
 * separately in unit and integration tests. Extends App which basically executed the code in the object as a script an Logger which provides logging
 * class.
 * Application reads two input sources - table of people and table of their salaries. Both stored as a separate csv file. The input is a join and aggregate
 * stored as a csv file. There are 4 stages of the processing:
 * 1) read - files via CSVReader class
 * 2) preprocess - there is a class doing this; it takes input DataFrames from Readers, tailors the data and returns two DataFrames.
 * 3) core - input and preprocessed DataFrames are first joined, then aggregated and result is returned as a DataFrame.
 * 4) write - resulting aggregates are written into a csv file.
 */
object Processor extends App with Logger {
  /**
   * initialising settings and configuration parameters
   */
  val configFile = if(System.getProperty("os.name").startsWith("Windows")) {
    logger.info("Detected Windows configuration")
    "rbm_config.windows.conf"
  } else {
    logger.info("Detected Linux configuration")
    "rbm_config.linux.conf"
  }

  val conf = new Setup(configFile)

  /**
   * checking all the configuration is available and valid. If not program exits with 1 exit code. Before doing so it also prints the parameters which are missing or invalid.
   */
  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }
  /**
   * printing all the parameters and their values.
   */
  conf.settings.printAllFields()

  /**
   * Creating implicit SparkSession used in the processing blocks.
   */
  implicit val sparkSession: SparkSession = getSparkSession(conf.settings.appName.get)

  /**
   * Creating readers for input data wrapped in a case class.
   */
  val inputReaders = InputData(
    rbm_activity = new CSVReader(conf.settings.inputPath.get + "rbm_activity_2019-01-25_mt.csv", header = true),
    rbm_billable_events = new CSVReader(conf.settings.inputPath.get + "rbm_billable_events_2019-10-06_mt.csv", header = true)
  )

  /**
   * creating staging step class for preprocessing/
   */
  val stage = new Stage()

  /**
   * creating class responsible for core of the processing.
   */
  val processingCore = new CoreLogicWithTransform()

  /**
   * Creating final writes storing resulting data into a CSV file.
   */
  val resultPaths = ResultPaths(conf.settings.lookupPath.get, conf.settings.outputPath.get)
  val resultWriter = new ResultWriter(resultPaths)
  /**
   * Creating a pipeline where all the steps are executed in the desired order. Processing steps classes
   * are passed as class parameters.
   */
  val pipeline = new Pipeline(inputReaders,stage,processingCore,resultWriter)


  /**
   * Pipeline is executed.
   */
  pipeline.run()

}
