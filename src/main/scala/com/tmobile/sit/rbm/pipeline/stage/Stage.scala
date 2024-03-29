package com.tmobile.sit.rbm.pipeline.stage

import com.tmobile.sit.rbm.pipeline.Logger
import org.apache.spark.sql.functions.{col, lit, split}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class trait/interface which needs to be implemented
 */
trait StageProcessing extends Logger{
  def preprocessActivity(input: DataFrame, file_natco_id: String) : DataFrame
  def preprocessEvents(input: DataFrame, file_natco_id: String, file_date: String) : DataFrame
  def preprocessNatCoMapping(input: DataFrame) : DataFrame
  def preprocessConversationTypeMapping(input: DataFrame) : DataFrame
  def preprocessContentDescriptionMapping(input: DataFrame) : DataFrame
  def preprocessAccUsersDaily(acc_uau_daily: DataFrame, rbmActivity: DataFrame,
                              file_date: String, file_natco_id: String,d_agent: DataFrame):DataFrame
}

/**
 * Stage class implementation
 */
class Stage  (implicit sparkSession: SparkSession) extends StageProcessing {

  import sparkSession.sqlContext.implicits._

  override def preprocessAccUsersDaily(acc_users_daily: DataFrame, rbmActivity: DataFrame,
                                       file_date: String, file_natco_id: String,d_agent: DataFrame ):DataFrame = {
    logger.info("Preprocessing UAU Accumulator")
    val users_today = rbmActivity
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .withColumn("Date", split(col("time"), "T").getItem(0))
      .withColumn("FileDate", lit(file_date))
      .withColumn("NatCo", lit(file_natco_id))
      .join(d_agent, Seq("Agent"), "left")
      .select("FileDate",  "Date", "NatCo","AgentID" ,"user_id")


    val inter = acc_users_daily
      .filter($"FileDate" =!= lit(file_date))
      .withColumn("Date", col("Date").cast("date"))
      .withColumn("FileDate", col("FileDate").cast("date"))
      .select("FileDate",  "Date", "NatCo", "AgentID","user_id")
      .orderBy("FileDate")


    if (users_today.count()==0) inter else inter.union(users_today).orderBy("FileDate")
  }

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
