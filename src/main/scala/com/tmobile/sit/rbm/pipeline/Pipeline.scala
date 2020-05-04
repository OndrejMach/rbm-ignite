package com.tmobile.sit.rbm.pipeline

/**
 * Spark processing pipeline definition. Class takes processing blocks (each processing block is a class) as parameters and executes them in the desired order.
 * @param inputData - raw input data
 * @param mappingData - static mapping data
 * @param fileMetaData - file metadata
 * @param persistentData - persistent dimensions/files which are updated
 * @param stage - class responsible for data preprocessing
 * @param core - class doing core of the processing on preprocessed data
 * @param writer - final writer storing data in the desired format (CSV here)
 */

class Pipeline(inputData: InputData, mappingData: MappingData, fileMetaData: FileMetaData,
               persistentData: PersistentData, stage: StageProcessing, core: ProcessingCore,
               writer: ResultWriter) {
  def run(): Unit = {

    val preprocessedData =
      PreprocessedData(
        stage.preprocessActivity(inputData.rbm_activity.read(),fileMetaData.file_natco_id),
        stage.preprocessEvents(inputData.rbm_billable_events.read(),fileMetaData.file_natco_id, fileMetaData.file_date),
        stage.preprocessNatCoMapping(mappingData.NatCoMapping.read()),
        stage.preprocessConversationTypeMapping(mappingData.ConversationTypeMapping.read()),
        stage.preprocessContentDescriptionMapping(mappingData.ContentDescriptionMapping.read()),
        stage.preprocessAccUsersDaily(persistentData.acc_users_daily,inputData.rbm_activity.read(),fileMetaData.file_date,fileMetaData.file_natco_id)
      )

    val result = core.process(preprocessedData, persistentData)

    writer.write(result)
  }

}
