package com.tmobile.sit.jobtemplate.pipeline

import com.tmobile.sit.common.writers.Writer

class Pipeline(inputData: InputData, stage: TemplateStageProcessing, core: ProcessingCore, writer: Writer) {
  def run(): Unit = {

    val preprocessedData = PreprocessedData(stage.preprocessPeople(inputData.people.read()),stage.preprocessSalaryInfo(inputData.salaryInfo.read()) )

    val result = core.process(preprocessedData)

    writer.writeData(result)
  }

}
