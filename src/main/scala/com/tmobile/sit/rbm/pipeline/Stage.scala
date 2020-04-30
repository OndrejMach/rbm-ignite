package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

/**
 * Class trait/interface which needs to be implemented
 */
trait StageProcessing extends Logger{
  def preprocessActivity(input: DataFrame, file_natco_id: String) : DataFrame
  def preprocessEvents(input: DataFrame, file_natco_id: String, file_date: String) : DataFrame
  def preprocessNatCoMapping(input: DataFrame) : DataFrame
  def preprocessConversationTypeMapping(input: DataFrame) : DataFrame
  def preprocessContentDescriptionMapping(input: DataFrame) : DataFrame
}

/**
 * Stage class implementation
 */
class Stage  (implicit sparkSession: SparkSession) extends StageProcessing {

  // Adding NatCo column based on file source
  override def preprocessActivity(rbmActivity: DataFrame, file_natco_id: String): DataFrame = {
    rbmActivity.withColumn("NatCo", lit(file_natco_id))
  }

  // Adding NatCo and FileDate column based on file source
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
