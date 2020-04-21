package com.tmobile.sit.rbm.pipeline

/**
 * Spark processing pipeline definition. Class takes processing blocks (each processing block is a class) as parameters and executes them in the desired order.
 * @param inputData - raw input data read by the readers
 * @param stage - class responsible for data preprocessing - stage
 * @param core - class doing core of the processing on preprocessed data
 * @param writer - final writer storing data in the desired format (CSV here)
 */

class Pipeline(inputData: InputData, mappingData: MappingData, fileMetaData: FileMetaData,
               stage: StageProcessing, core: ProcessingCore, writer: ResultWriter) {
  def run(): Unit = {

    val preprocessedData =
      PreprocessedData(
        stage.preprocessActivity(inputData.rbm_activity.read(),fileMetaData.file_natco_id),
        stage.preprocessEvents(inputData.rbm_billable_events.read(),fileMetaData.file_natco_id, fileMetaData.file_date),
        stage.preprocessNatCoMapping(mappingData.NatCoMapping.read()),
        stage.preprocessConversationTypeMapping(mappingData.ConversationTypeMapping.read()),
        stage.preprocessContentDescriptionMapping(mappingData.ContentDescriptionMapping.read())
      )

    val result = core.process(preprocessedData)

    writer.write(result)
  }

}
