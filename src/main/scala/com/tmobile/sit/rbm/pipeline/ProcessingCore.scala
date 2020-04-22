package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.functions.{col, count, countDistinct, row_number, split, sum, when,year,month, concat_ws, avg}
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
      .drop("type")
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

  def getMessagesByType(rbm_activity: DataFrame, d_natco: DataFrame,
                        d_content_type: DataFrame, d_agent: DataFrame): DataFrame = {
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
      .join(d_content_type, d_content_type("OriginalContent") === activitiesGrouped_AllDirections("type"), "left")
      .join(d_agent, d_agent("Agent") === activitiesGrouped_AllDirections("Agent"), "left")
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

  def getNoOfConvAndSM(rbm_billable_events: DataFrame, d_natco: DataFrame,
                       d_agent: DataFrame):DataFrame = {
    // Count MT and MO messages per group (ap, pa, sm)
    val eventsByMessageType = rbm_billable_events
      .withColumn("Date", split(col("FileDate"), " ").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type", "duration")
      .groupBy("Date", "NatCo", "Agent", "type")
      .agg(count("type").alias("Count"),
        avg("duration").alias("AverageDuration"))
      .withColumn("TypeOfConvID",
        when(col("type") === "a2p_conversation", "1")
          .otherwise(when(col("type") === "p2a_conversation", "2")
            .otherwise(when(col("type") === "single_message", "1"))))

    //eventsByMessageType.show()

    val eventsMessageAllTypes = eventsByMessageType
        .filter(col("type") =!= "single_message")
        .withColumnRenamed("Count", "NoOfConv")
        .withColumnRenamed("type", "TypeOfConv")
        .join(eventsByMessageType
          .filter(col("type") === "single_message")
          .withColumnRenamed("AverageDuration","AverageDurationSM")
          .withColumnRenamed("type", "TypeOfSM"),
        Seq("Date", "NatCo", "Agent", "TypeOfConvID"),
        "fullouter")
        .withColumn("NoOfSM", when(col("Count").isNull, 0).otherwise(col("Count")))
        .withColumn("AverageDuration",
          when(col("AverageDuration").isNull, col("AverageDurationSM"))
            .otherwise(col("AverageDuration")))
        .drop("Count", "AverageDurationSM")

    //eventsMessageAllTypes.show()

    eventsMessageAllTypes
      .join(d_agent, d_agent("Agent") === eventsMessageAllTypes("Agent"), "left")
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
      .select("Date", "NatCoID", "AgentID", "TypeOfConvID", "AverageDuration", "NoOfConv", /*"TypeOfSM",*/"NoOfSM")

  }

  def getNoOfMessByTypeOfConv(rbm_billable_events: DataFrame,
                              d_natco: DataFrame,
                              d_agent: DataFrame):DataFrame = {

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
      .join(d_agent, d_agent("Agent") === eventsByConvType("Agent"), "left")
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

  def getUAU(rbm_activity: DataFrame, d_natco: DataFrame):DataFrame = {

    import sparkSession.implicits._
    //TODO: fix for this
    val rbm_acivity_YMD = rbm_activity
      .withColumn("NatCoID",
        when(col("NatCo") === "mt", "1")
          .otherwise(when(col("NatCo") === "cg", "2")
            .otherwise(when(col("NatCo") === "st", "3")
              .otherwise(when(col("NatCo") === "cr", "4")
                .otherwise("-1")))))
      .withColumn("Date", split(col("time"), " ").getItem(0))
      .withColumn("Year",year(col("time")))
      .withColumn("Month",month(col("time")))
      .withColumn("YearMonth", concat_ws("-",year(col("time")),month(col("time"))))
      .select("Date", "Year","YearMonth", "Month", "NatCoID", "user_id")

    val rbm_activity_daily = rbm_acivity_YMD
      .select("Date", "YearMonth", "Year", "NatCoID", "user_id")
      .groupBy("Date", "YearMonth", "Year", "NatCoID")
      .agg(countDistinct("user_id").alias("UAU_daily"))

    val rbm_activity_monthly = rbm_acivity_YMD
      .groupBy("YearMonth", "NatCoID")
      .agg(countDistinct("user_id").alias("UAU_monthly"))

    val rbm_activity_yearly = rbm_acivity_YMD
      .groupBy("Year", "NatCoID")
      .agg(countDistinct("user_id").alias("UAU_yearly"))

    val rbm_activity_total = rbm_acivity_YMD
      .groupBy("NatCoID")
      .agg(countDistinct("user_id").alias("UAU_total"))

    val rbm_activity_daily_final = rbm_activity_daily.as("d")
        .join(rbm_activity_monthly.as("m"),
          $"d.YearMonth" === $"m.YearMonth" &&
          $"d.NatCoID" === $"m.NatCoID",
          "leftouter")
        .join(rbm_activity_yearly.as("y"),
          $"d.Year" === $"y.Year" &&
          $"d.NatCoID" === $"y.NatCoID",
          "leftouter")
        .join(rbm_activity_total.as("t"),
          $"d.NatCoID" === $"t.NatCoID",
        "leftouter")
        .select("Date", "d.NatCoID","UAU_daily", "UAU_monthly", "UAU_yearly", "UAU_total")

    rbm_activity_daily_final
    }

  /**
   * The process class creates the output files as dimensions and facts
   */
  override def process(preprocessedData: PreprocessedData): OutputData = {
    logger.info("Executing  processing core")

    //TODO: Some joins like with rbm_activity_total or with d_natco are seen as cross joins. Can this be avoided?
    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")

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
    val f_uau = getUAU(preprocessedData.rbm_activity, d_natco /*Not actually used. Used static mapping instead.*/)

    //d_agent.show()
    //f_message_content.show()
    //f_uau.show()

    OutputData(d_natco,
      d_content_type,
      d_conversation_type,
      d_agent,
      d_agent_owner,
      f_message_content,
      f_conversations_and_sm,
      f_message_conversation,
      f_uau)
  }
}