package com.tmobile.sit.rbm.pipeline.core


import com.tmobile.sit.common.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, bround, count, countDistinct, date_format, lit, regexp_replace, split, sum, when, year}

/**
 * Class trait/interface which needs to be implemented
 */
trait FactProcessing extends Logger{
  def process_F_Message_Content(rbm_activity: DataFrame, d_natco: DataFrame,
                                d_content_type: DataFrame, d_agent: DataFrame): DataFrame

  def process_F_Message_Conversation(rbm_billable_events: DataFrame,
    d_natco: DataFrame, d_agent: DataFrame):DataFrame

  def process_F_Conversation_And_SM(rbm_billable_events: DataFrame, d_natco: DataFrame,
                                    d_agent: DataFrame):DataFrame

  def process_F_UAU_Daily(rbm_activity: DataFrame, d_natco: DataFrame):DataFrame

}


class Fact(implicit sparkSession: SparkSession) extends FactProcessing {



  import sparkSession.sqlContext.implicits._

  override def process_F_Message_Content(rbm_activity: DataFrame, d_natco: DataFrame,
                                         d_content_type: DataFrame, d_agent: DataFrame): DataFrame = {
    logger.info("Processing f_message_content for day")

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
    activitiesGrouped_AllDirections.as("main")
      .join(d_content_type, d_content_type("OriginalContent") === activitiesGrouped_AllDirections("type"), "left")
      .join(d_agent, d_agent("Agent") === activitiesGrouped_AllDirections("Agent"), "left")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .select("Date", "NatCoID", "ContentID", "AgentID", "MT_MessagesByType", "MO_MessagesByType", "MTMO_MessagesByType")
  }

  override def process_F_Message_Conversation(rbm_billable_events: DataFrame,
                                              d_natco: DataFrame,
                                              d_agent: DataFrame):DataFrame = {
    logger.info("Processing f_message_conversation for day")

    val eventsByConvType = rbm_billable_events
      //TODO: Date is open topic since it's the one gotten from the filename
      .withColumn("Date", split(col("FileDate"), " ").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type", "mt_messages", "mo_messages")
      .groupBy("Date", "NatCo", "Agent", "type")
      .agg(sum("mo_messages").alias("MO_messages"),
        sum("mt_messages").alias("MT_messages"))
      .withColumn("MTMO_messages", col("mo_messages") + col("mt_messages"))

    val eventsByConvAllTypes = eventsByConvType.as("main")
      .filter(col("type") =!= "single_message")
      .join(d_agent, d_agent("Agent") === eventsByConvType("Agent"), "left")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .withColumn("TypeOfConvID",
        when(col("type") === "a2p_conversation", "1")
          .otherwise(when(col("type") === "p2a_conversation", "2")))
      .select("Date","NatCoID","AgentID","TypeOfConvID","MO_messages", "MT_messages","MTMO_messages")

    eventsByConvAllTypes
  }

  override def process_F_Conversation_And_SM(rbm_billable_events: DataFrame, d_natco: DataFrame,
                                             d_agent: DataFrame):DataFrame = {
    logger.info("Processing f_conversation_and_sm for day")

    // Calculate a2p + p2a statistics
    /*
    val conversationEventsMerged = rbm_billable_events
      .filter(col("type") =!= "single_message")
      .withColumn("Date", split(col("FileDate"), " ").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type", "duration")
      .groupBy("Date", "NatCo", "Agent")
      .agg(avg("duration").alias("AverageDurationConv") //Merged duration
        /*,count("type").alias("NoOfConvMerged") //Merged count not needed */)
      .orderBy("Agent")
    */
    //conversationEventsMerged.show()

    // Calculate separate a2p and p2a statistics
    val conversationEventsSplit = rbm_billable_events
      .filter(col("type") =!= "single_message")
      .withColumn("Date", split(col("FileDate"), " ").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type", "duration")
      .groupBy("Date", "NatCo", "Agent", "type")
      .agg(bround(avg("duration"),2).alias("AverageDurationConv"),
        count("type").alias("NoOfConv"))
      .withColumn("TypeOfConvID",
        when(col("type") === "a2p_conversation", "1")
          .otherwise(when(col("type") === "p2a_conversation", "2")))
      .withColumnRenamed("type","TypeOfConv")
      .orderBy("Agent")

    //conversationEventsSplit.show()

    // Calculate sm statistics
    val singleMessageEvents = rbm_billable_events
      .filter(col("type") === "single_message")
      .withColumn("Date", split(col("FileDate"), " ").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type", "duration")
      .groupBy("Date", "NatCo", "Agent","type")
      .agg(/*bround(avg("duration"),2).alias("AverageDurationSM"),*/
        count("type").alias("NoOfSM"))
      .withColumn("AverageDurationSM", lit(null))
      .withColumn("TypeOfConvID",lit("1"))
      .withColumnRenamed("type","TypeOfSM")
      .orderBy("Agent")

    //singleMessageEvents.show()

    val eventsMessageAllTypes = conversationEventsSplit
      .join(singleMessageEvents,
         Seq("Date", "NatCo", "Agent", "TypeOfConvID"),
        "fullouter")
      .withColumn("NoOfSM", when(col("NoOfSM").isNull, 0).otherwise(col("NoOfSM")))
      .withColumn("AverageDuration",
        when(col("AverageDurationConv").isNull, col("AverageDurationSM"))
          .otherwise(col("AverageDurationConv")))
      .drop("Count", "AverageDurationSM", "AverageDurationConv")
      .orderBy("Agent")

    //eventsMessageAllTypes.show()

    val fact = eventsMessageAllTypes.as("main")
      .join(d_agent, d_agent("Agent") === eventsMessageAllTypes("Agent"), "left")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      //fix for a2p single messages which are not part of a conversation
      .withColumn("NoOfConv",
        when(col("TypeOfSM") === "single_message" && col("TypeOfConv").isNull, 0)
          .otherwise(col("NoOfConv")))
      .withColumn("AverageDuration", regexp_replace(col("AverageDuration"), lit("\\."), lit(",")))
      .select("Date", "NatCoID", "AgentID", "TypeOfConvID", "AverageDuration", "NoOfConv", /*"TypeOfSM",*/"NoOfSM")

    //fact.show()

    fact
  }

  override def process_F_UAU_Daily(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame = {
    logger.info("Processing f_uau_daily")

    new_acc_uau_daily.as("main")
      .select("Date", "NatCo", "user_id")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .withColumn("Date", col("Date").cast("date"))
      .groupBy("Date", "NatCoID")
      .agg(countDistinct("user_id").alias("UAU_daily"))
      .select("Date", "NatCoID", "UAU_daily")
      .orderBy("Date")

  }

  def process_F_UAU_Monthly(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame = {
    logger.info("Processing f_uau_monthly")

    new_acc_uau_daily.as("main")
      .select("Date", "NatCo", "user_id")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .withColumn("YearMonth", date_format(col("Date"),"yyyy-MM"))
      .groupBy("YearMonth", "NatCoID")
      .agg(countDistinct("user_id").alias("UAU_monthly"))
      .select("YearMonth", "NatCoID", "UAU_monthly")
  }

  def process_F_UAU_Yearly(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame = {
    logger.info("Processing f_uau_yearly")

    new_acc_uau_daily.as("main")
      .select("Date", "NatCo", "user_id")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .withColumn("Year", year(col("Date")))
      .groupBy("Year", "NatCoID")
      .agg(countDistinct("user_id").alias("UAU_yearly"))
      .select("Year", "NatCoID", "UAU_yearly")
  }

  def process_F_UAU_Total(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame = {
    logger.info("Processing f_uau_total")

    new_acc_uau_daily.as("main")
      .select("Date", "NatCo", "user_id")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .groupBy( "NatCoID")
      .agg(countDistinct("user_id").alias("UAU_total"))
      .select( "NatCoID", "UAU_total")
  }
}
