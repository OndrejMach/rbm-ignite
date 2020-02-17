package com.tmobile.sit.jobtemplate

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.readers.{CSVReader, Reader}
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.jobtemplate.config.Setup
import com.tmobile.sit.jobtemplate.pipeline.{CoreLogicWithTransform, InputData, Pipeline, TemplateStage}

case class Inputs(input1: Reader, input2: Reader, input3: Reader)

object Processor extends App with Logger {
  val conf = new Setup()

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()


  implicit val sparkSession = getSparkSession(conf.settings.appName.get)

  val inputReaders = InputData(
    people = new CSVReader(conf.settings.inputPathPeople.get, header = true),
    salaryInfo = new CSVReader(conf.settings.inputPathSalaryInfo.get, header = true)
  )

  val stage = new TemplateStage()
  val processingCore = new CoreLogicWithTransform()


  val resultWriter = new CSVWriter(conf.settings.outputPath.get, writeHeader = true)


  val pipeline = new Pipeline(inputReaders,stage,processingCore,resultWriter)

  pipeline.run()

}
