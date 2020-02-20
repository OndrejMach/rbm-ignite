package com.tmobile.sit.jobtemplate

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.CSVReader
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.jobtemplate.config.Setup
import com.tmobile.sit.jobtemplate.pipeline.{CoreLogicWithTransform, InputData, Pipeline, TemplateStage}

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
  val conf = new Setup()

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
  implicit val sparkSession = getSparkSession(conf.settings.appName.get)

  /**
   * Creating readers for input data wrapped in a case class.
   */
  val inputReaders = InputData(
    people = new CSVReader(conf.settings.inputPathPeople.get, header = true),
    salaryInfo = new CSVReader(conf.settings.inputPathSalaryInfo.get, header = true)
  )

  /**
   * creating staging step class for preprocessing/
   */
  val stage = new TemplateStage()

  /**
   * creating class responsible for core of the processing.
   */
  val processingCore = new CoreLogicWithTransform()

  /**
   * Creating final writes storing resulting data into a CSV file.
   */
  val resultWriter = new CSVWriter(conf.settings.outputPath.get, writeHeader = true)

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
