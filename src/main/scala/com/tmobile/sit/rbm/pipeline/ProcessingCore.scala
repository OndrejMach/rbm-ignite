package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.functions.{col, split, row_number, count, sum, when, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window


/**
 * trait defining interface for the main processing block
 */

trait ProcessingCore extends Logger {
  def process(preprocessedData: PreprocessedData) : OutputData
}

/**
 * This class implements core of the processing, for each important processing steps there is a method (two here for join and final aggregation).
 * This split should help with readability and structure. Core of the processing is method process which is basically ran first.
 */
class CoreLogicWithTransform (implicit sparkSession: SparkSession) extends ProcessingCore {

  def getContentMapping(rbm_activity: DataFrame): DataFrame  = {
    // Select distinct activity_id and type pairs
    rbm_activity.select("activity_id", "type").distinct()
  }

  def getAgentMapping(rbm_activity: DataFrame, rbm_billable_events: DataFrame): DataFrame = {

    rbm_activity.select("agent_id")
      .union(rbm_billable_events.select("agent_id"))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .drop("agent_id")
      .distinct()
      .withColumn("AgentID", row_number.over(Window.orderBy("Agent")))
  }

  def getMessagesByType(rbm_activity: DataFrame, NatCoMapping: DataFrame,
                        ContentMapping: DataFrame, AgentMapping: DataFrame): DataFrame = {
    // Count MT and MO messages per group
     val activityGroupedByDirection = rbm_activity
      .withColumn("Date", split(col("time"), " ").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type", "direction")
      .groupBy("Date", "NatCo", "Agent", "type","direction")
      .agg(count("type").alias("Count"))

    val activitiesGrouped_AllDirections = activityGroupedByDirection
      // Get MT+MO counts by not grouping on direction
      .groupBy("Date", "NatCo", "Agent", "type")
      .agg(sum("Count").alias("MTMO_MessagesByType"))
      // Add MT message counts from previous step
      .join(activityGroupedByDirection.filter(col("direction") === "mt"),
        Seq("Date", "NatCo", "Agent", "type"), "left")
      .withColumn("MT_MessagesByType", when(col("Count").isNull, 0).otherwise(col("Count")))
      .drop("direction", "Count")
      // Add MO message counts from previous step
      .join(activityGroupedByDirection.filter(col("direction") === "mo"),
        Seq("Date", "NatCo", "Agent", "type"), "left")
      .withColumn("MO_MessagesByType", when(col("Count").isNull, 0).otherwise(col("Count")))
      .drop("direction", "Count")

    // Create final MessagesByType fact by joining on Lookups to get the FKs
    activitiesGrouped_AllDirections
      .join(ContentMapping, ContentMapping("type") === activitiesGrouped_AllDirections("type"), "left")
      .join(AgentMapping, AgentMapping("Agent") === activitiesGrouped_AllDirections("Agent"), "left")
      .withColumn("ContentID", col("activity_id"))
      //TODO: change this workaround to work with lookup table
      .withColumn("NatCoID",
        when(col("NatCo") === "mt", "1")
        .otherwise(when(col("NatCo") === "cg", "2")
          .otherwise(when(col("NatCo") === "st", "3")
            .otherwise(when(col("NatCo") === "cr", "4")
              .otherwise("-1")))))
      .drop("Agent", "type","NatCo", "activity_id")
      .select("Date", "NatCoID", "ContentID", "AgentID", "MT_MessagesByType", "MO_MessagesByType", "MTMO_MessagesByType")

  }

  def getNoOfConvAndSM(rbm_billable_events: DataFrame, NatCoMapping: DataFrame,
                       AgentMapping: DataFrame):DataFrame = {
    // Count MT and MO messages per group
    val eventsByMessageType = rbm_billable_events
      //TODO: Determine what date should be used here
      .withColumn("Date", split(col("FileDate"), " ").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type")
      .groupBy("Date", "NatCo", "Agent", "type")
      .agg(count("type").alias("Count"))
      .withColumn("TypeOfConvID",
        when(col("type") === "a2p_conversation", "1")
          .otherwise(when(col("type") === "p2a_conversation", "2")))

    val eventsMessageAllTypes = eventsByMessageType
        .filter(col("type") =!= "single_message")
        .withColumnRenamed("Count", "NoOfConv")
        .withColumnRenamed("type", "TypeOfConv")
        .join(eventsByMessageType
          .filter(col("type") === "single_message")
          .withColumn("TypeOfConvID",lit("1"))
          .withColumnRenamed("type", "TypeOfSM"),
        Seq("Date", "NatCo", "Agent", "TypeOfConvID"),
        "fullouter")
        .withColumn("NoOfSM", when(col("Count").isNull, 0).otherwise(col("Count")))
        .drop("Count")

    eventsMessageAllTypes.show()

    eventsMessageAllTypes
      .join(AgentMapping, AgentMapping("Agent") === eventsMessageAllTypes("Agent"), "left")
      //TODO: change this workaround to work with lookup table
      .withColumn("NatCoID",
        when(col("NatCo") === "mt", "1")
          .otherwise(when(col("NatCo") === "cg", "2")
            .otherwise(when(col("NatCo") === "st", "3")
              .otherwise(when(col("NatCo") === "cr", "4")
                .otherwise("-1")))))
      //fix for a2p single messages which are not part of a conversation
      .withColumn("NoOfConv",
        when(col("TypeOfSM") === "single_message" && col("NoOfConv").isNull, "0")
          .otherwise(col("NoOfConv")))
      .select("Date", "NatCoID", "AgentID", "TypeOfConvID", /*"TypeOfConv",*/ "NoOfConv", /*"TypeOfSM",*/"NoOfSM")

  }

  def getNoOfMessByTypeOfConv(rbm_billable_events: DataFrame,
                              NatCoMapping: DataFrame,
                              AgentMapping: DataFrame):DataFrame = {

    val eventsByConvType = rbm_billable_events
      //TODO: Determine what date should be used here
      .withColumn("Date", split(col("FileDate"), " ").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type", "mt_messages", "mo_messages")
      .groupBy("Date", "NatCo", "Agent", "type")
      .agg(sum("mo_messages").alias("MO_messages"),
        sum("mt_messages").alias("MT_messages"))
      .withColumn("MTMO_messages", col("mo_messages") + col("mt_messages"))

    val eventsByConvAllTypes = eventsByConvType
      .filter(col("type") =!= "single_message")
      .join(AgentMapping, AgentMapping("Agent") === eventsByConvType("Agent"), "left")
      //TODO: change this workaround to work with lookup table
      .withColumn("NatCoID",
        when(col("NatCo") === "mt", "1")
          .otherwise(when(col("NatCo") === "cg", "2")
            .otherwise(when(col("NatCo") === "st", "3")
              .otherwise(when(col("NatCo") === "cr", "4")
                .otherwise("-1")))))
      .withColumn("TypeOfConvID",
        when(col("type") === "a2p_conversation", "1")
        .otherwise(when(col("type") === "p2a_conversation", "2")))
      .drop("Agent","NatCo")
      .select("Date","NatCoID","AgentID","TypeOfConvID","MO_messages", "MT_messages","MTMO_messages")

    eventsByConvAllTypes
  }

  /**
   * The process class creates the output files as follows:
   * NatCoMapping - static mapping coming from Stage layer
   * ContentMapping - based on rbm_activity data
   * AgentMapping - based on rbm_activity and rbm_billable_events data
   * MessagesByType - based on rbm_activity data and lookups
   * NoOfConvAndSM - based on rbm_billable_events data and lookups
   * NoOfMessByTypeOfConv - based on rbm_billable_events data and lookups
   */
  override def process(preprocessedData: PreprocessedData): OutputData = {
    logger.info("Executing  processing core")

    val d_natco = preprocessedData.NatCoMapping
    val d_conversation_type = preprocessedData.NatCoMapping
    val d_content_type = getContentMapping(preprocessedData.rbm_activity)
    val d_agent = getAgentMapping(preprocessedData.rbm_activity,preprocessedData.rbm_billable_events)

    val f_message_content = getMessagesByType(preprocessedData.rbm_activity,
      d_natco, /*Not actually used because of compilation bug. Used static mapping instead.*/
      d_content_type,
      d_agent)

    val f_message_conversation = getNoOfMessByTypeOfConv(preprocessedData.rbm_billable_events,
      d_natco, /*Not actually used because of compilation bug. Used static mapping instead.*/
      d_agent)

    val f_conversations_and_sm = getNoOfConvAndSM(preprocessedData.rbm_billable_events,
      d_natco, /*Not actually used because of compilation bug. Used static mapping instead.*/
      d_agent)

    //NoOfConvAndSM.show()

    OutputData(d_natco,
      d_content_type,
      d_conversation_type,
      d_agent,
      f_message_content,
      f_conversations_and_sm,
      f_message_conversation)
  }
}