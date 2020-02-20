package com.tmobile.sit.jobtemplate.pipeline

import com.tmobile.sit.common.writers.Writer

/**
 * Spark processing pipeline definition. Class takes processing blocks (each processing block is a class) as parameters and executes them in the desired order.
 * @param inputData - raw input data read by the readers
 * @param stage - class responsible for data preprocessing - stage
 * @param core - class doing core of the processing on preprocessed data
 * @param writer - final writer storing data in the desired format (CSV here)
 */

class Pipeline(inputData: InputData, stage: TemplateStageProcessing, core: ProcessingCore, writer: Writer) {
  def run(): Unit = {

    val preprocessedData = PreprocessedData(stage.preprocessPeople(inputData.people.read()),stage.preprocessSalaryInfo(inputData.salaryInfo.read()) )

    val result = core.process(preprocessedData)

    writer.writeData(result)
  }

}
