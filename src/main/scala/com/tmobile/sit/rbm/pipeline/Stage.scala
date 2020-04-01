package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

/**
 * A trait defining preprocessing of the input data. There are two method for preprocessing of each data source.
 */

trait StageProcessing extends Logger{
  def preprocessActivity(input: DataFrame, file_natco_id: String) : DataFrame
  def preprocessEvents(input: DataFrame, file_natco_id: String, file_date: String) : DataFrame
  def preprocessNatCoMapping(input: DataFrame) : DataFrame
  def  preprocessConversationTypeMapping(input: DataFrame) : DataFrame
  def  preprocessContentDescriptionMapping(input: DataFrame) : DataFrame
}

/**
 * Preprocessing implementation, two methods - one for people table preprocessing and one for salaryInfo preprocessing.
 */
class Stage  (implicit sparkSession: SparkSession) extends StageProcessing {
  import sparkSession.sqlContext.implicits._
  /**
   * Prepares people data for processing. Basically a simple step dropping one column and filtering data based on the ID value.
   * @param rbmActivity - a DataFrame containing people table read from CSV
   * @return - people data tuned and preprocessed.
   */


  override def preprocessActivity(rbmActivity: DataFrame, file_natco_id: String): DataFrame = {
    rbmActivity.withColumn("NatCo", lit(file_natco_id))
  }

  /**
   * Preprocessing of the salaryInfo data. Selects only two columns needed and adds one more 'isValid' with a default value 'true'
   * @param rbmEvents - input salaryInfo table from csv.
   * @return - preprocessed data as DataFrame.
   */
  override def preprocessEvents(rbmEvents: DataFrame, file_natco_id: String, file_date: String) : DataFrame = {
    rbmEvents.withColumn("NatCo", lit(file_natco_id))
      .withColumn("FileDate", lit(file_date))
  }

  override def preprocessNatCoMapping(NatCoMapping: DataFrame): DataFrame = {
    NatCoMapping
  }

  override def preprocessConversationTypeMapping(conversationTypeMapping: DataFrame): DataFrame = {
    conversationTypeMapping
  }

  override def preprocessContentDescriptionMapping(contentDescriptionMapping: DataFrame): DataFrame = {
    contentDescriptionMapping
  }
}
