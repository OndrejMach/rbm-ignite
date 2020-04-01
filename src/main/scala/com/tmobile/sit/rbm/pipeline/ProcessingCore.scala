package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.functions.{col, count, lit, row_number, split, sum, when}
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

  def getContentMapping(rbm_activity: DataFrame, ContentDescriptionMapping: DataFrame): DataFrame  = {
    // Select distinct activity_id and type pairs
    rbm_activity.select("activity_id", "type").distinct()
      .join(ContentDescriptionMapping,
        rbm_activity("type") === ContentDescriptionMapping("OriginalContent"),
      "left")
      .drop("type", "OriginalContent")
      .withColumnRenamed("activity_id","ContentID")
  }

  def getAgentMapping(rbm_activity: DataFrame, rbm_billable_events: DataFrame, d_agent_owner: DataFrame): DataFrame = {

    //Get distinct agents from both sources
    val distinctAgents = rbm_activity.select("agent_id")
      .union(rbm_billable_events.select("agent_id"))
      .distinct()
      .withColumn("AgentID", row_number.over(Window.orderBy("agent_id")))

    // Get distinct list of Agents and Owners from the events file because only there we have events and owners
    val agentsWithOwners = rbm_billable_events
      .select("agent_id", "agent_owner")
      .distinct()

    //agentsWithOwners.show()
    // Join to get AgentOwnerID
    distinctAgents
      .join(agentsWithOwners, distinctAgents("agent_id") === agentsWithOwners("agent_id"), "left")
      .withColumn("Agent",split(distinctAgents("agent_id"), "@").getItem(0))
      .withColumn("AgentOwner",split(col("agent_owner"), "@").getItem(0))
      .join(d_agent_owner, split(col("agent_owner"), "@").getItem(0) === d_agent_owner("AgentOwner"), "left")
      .select("AgentID", "AgentOwnerID", "Agent")
      //
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
      .join(ContentMapping, ContentMapping("Content") === activitiesGrouped_AllDirections("type"), "left")
      .join(AgentMapping, AgentMapping("Agent") === activitiesGrouped_AllDirections("Agent"), "left")
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
        when(col("TypeOfSM") === "single_message" && col("TypeOfConv").isNull, 0)
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

  def getAgentOwners(rbm_billable_events: DataFrame):DataFrame = {
    rbm_billable_events
      .withColumn("AgentOwner", split(col("agent_owner"), "@").getItem(0))
      .select("AgentOwner")
      .distinct()
      .withColumn("AgentOwnerID", row_number.over(Window.orderBy("AgentOwner")))
      .select("AgentOwnerID", "AgentOwner") //Order columns
  }

  /**
   * The process class creates the output files as dimensions and facts
   */
  override def process(preprocessedData: PreprocessedData): OutputData = {
    logger.info("Executing  processing core")

    val d_natco = preprocessedData.NatCoMapping
    val d_conversation_type = preprocessedData.ConversationTypeMapping
    val d_content_type = getContentMapping(preprocessedData.rbm_activity, preprocessedData.ContentDescriptionMapping)
    val d_agent_owner = getAgentOwners(preprocessedData.rbm_billable_events)
    val d_agent = getAgentMapping(preprocessedData.rbm_activity,
      preprocessedData.rbm_billable_events,
      d_agent_owner)
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

    //d_agent.show()
    //d_agent_owner.show()

    OutputData(d_natco,
      d_content_type,
      d_conversation_type,
      d_agent,
      d_agent_owner,
      f_message_content,
      f_conversations_and_sm,
      f_message_conversation)
  }
}