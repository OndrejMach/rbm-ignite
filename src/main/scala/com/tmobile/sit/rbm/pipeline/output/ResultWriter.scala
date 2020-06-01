package com.tmobile.sit.rbm.pipeline.output

import com.tmobile.sit.common.Logger
import com.tmobile.sit.common.writers.CSVWriter
import com.tmobile.sit.rbm.data.{FileMetaData, OutputData, ResultPaths}
import org.apache.spark.sql.SparkSession

trait Writer extends Logger{
  def write(output: OutputData): Unit
}
/**
 * The ResultWrite class is an implementation of the CSVWriter over a set of output files
 * required by the RBM pipeline. It takes into consideration the file metadata for the current
 * file date and natco, as well as the ResultsPath class because it's writing both output and
 * lookup files for the next iteration
 */
class ResultWriter(resultPaths: ResultPaths, fileMetaData: FileMetaData) (implicit sparkSession: SparkSession) extends Writer {
  override def write(outputData: OutputData) =
  {
    logger.info("Writing output files")

    //val fileSuffix = fileMetaData.file_date.replace("-","")+"_"+fileMetaData.file_natco_id

    CSVWriter(outputData.d_natco, resultPaths.outputPath+"d_natco.csv", delimiter = ";").writeData()
    CSVWriter(outputData.d_content_type, resultPaths.outputPath+"d_content_type.csv", delimiter = ";").writeData()
    CSVWriter(outputData.d_conversation_type, resultPaths.outputPath+"d_conversation_type.csv", delimiter = ";").writeData()
    CSVWriter(outputData.d_agent, resultPaths.outputPath+"d_agent.csv", delimiter = ";").writeData()
    CSVWriter(outputData.d_agent_owner, resultPaths.outputPath+"d_agent_owner.csv", delimiter = ";").writeData()
    CSVWriter(outputData.f_message_content, resultPaths.outputPath+s"f_message_content_${fileMetaData.file_natco_id}.csv", delimiter = ";").writeData()
    CSVWriter(outputData.f_conversations_and_sm, resultPaths.outputPath+s"f_conversations_and_sm_${fileMetaData.file_natco_id}.csv", delimiter = ";").writeData()
    CSVWriter(outputData.f_message_conversation, resultPaths.outputPath+s"f_message_conversation_${fileMetaData.file_natco_id}.csv", delimiter = ";").writeData()
    CSVWriter(outputData.f_uau_daily, resultPaths.outputPath+s"f_uau_daily_${fileMetaData.file_natco_id}.csv", delimiter = ";").writeData()
    CSVWriter(outputData.f_uau_monthly, resultPaths.outputPath+s"f_uau_monthly_${fileMetaData.file_natco_id}.csv", delimiter = ";").writeData()
    CSVWriter(outputData.f_uau_yearly, resultPaths.outputPath+s"f_uau_yearly_${fileMetaData.file_natco_id}.csv", delimiter = ";").writeData()
    CSVWriter(outputData.f_uau_total, resultPaths.outputPath+s"f_uau_total_${fileMetaData.file_natco_id}.csv", delimiter = ";").writeData()
    CSVWriter(outputData.new_acc_uau_daily, resultPaths.lookupPath+s"acc_users_daily_${fileMetaData.file_natco_id}.csv", delimiter = ";").writeData()
  }
}