package com.tmobile.sit.rbm.pipeline.core


import com.tmobile.sit.rbm.pipeline.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, bround, col, count, countDistinct, date_format, lit, regexp_replace, split, sum, upper, when, year}
import org.apache.spark.sql.types.StringType

/**
 * Class trait/interface which needs to be implemented for creating the daily fact tables
 */
trait FactProcessing extends Logger{
  def process_F_Message_Content(rbm_activity: DataFrame, d_natco: DataFrame,
                                d_content_type: DataFrame, d_agent: DataFrame): DataFrame

  def process_F_Message_Conversation(rbm_billable_events: DataFrame,
    d_natco: DataFrame, d_agent: DataFrame):DataFrame

  def process_F_Conversation_And_SM(rbm_billable_events: DataFrame, d_natco: DataFrame,
                                    d_agent: DataFrame):DataFrame

  def process_F_UAU_Daily(rbm_activity: DataFrame, d_natco: DataFrame):DataFrame
  def process_F_UAU_Monthly(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame
  def process_F_UAU_Yearly(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame
  def process_F_UAU_Total(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame
}

class Fact(implicit sparkSession: SparkSession) extends FactProcessing {

  import sparkSession.sqlContext.implicits._

  override def process_F_Message_Content(rbm_activity: DataFrame, d_natco: DataFrame,
                                         d_content_type: DataFrame, d_agent: DataFrame): DataFrame = {
    logger.info("Processing f_message_content for day")

    // Count MT and MO messages per group
    val activityGroupedByDirection = rbm_activity
      .withColumn("Date", split(col("time"), "T").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type", "direction")
      .groupBy("Date", "NatCo", "Agent", "type","direction")
      .agg(count("*").alias("Count"))

    val activitiesGrouped_AllDirections = activityGroupedByDirection
      // Get MT+MO counts by not grouping on direction
      .groupBy("Date", "NatCo", "Agent", "type")
      .agg(sum("Count").alias("MTMO_MessagesByType"))
      // Add MT message counts from previous step
      .join(activityGroupedByDirection.filter(upper(col("direction")) === "MT"),
        Seq("Date", "NatCo", "Agent", "type"), "left")
      .withColumn("MT_MessagesByType", when(col("Count").isNull, 0).otherwise(col("Count")))
      .select("Date", "NatCo", "Agent", "type","MT_MessagesByType", "MTMO_MessagesByType" )
      // Add MO message counts from previous step
      .join(activityGroupedByDirection.filter(upper(col("direction")) === "MO"),
        Seq("Date", "NatCo", "Agent", "type"), "left")
      .withColumn("MO_MessagesByType", when(col("Count").isNull, 0).otherwise(col("Count")))
      .select("Date", "NatCo", "Agent", "type","MT_MessagesByType", "MO_MessagesByType", "MTMO_MessagesByType" )

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
    //rbm_billable_events.select(split(col("start_time").cast(StringType), "T").getItem(0)).show(1000, false)
    val eventsByConvType = rbm_billable_events
      //RESOLVED: Date is open topic since it's the one gotten from the filename
      //.withColumn("Date", split(col("FileDate"), " ").getItem(0))
      .withColumn("Date", split(col("start_time"), "T").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type", "mt_messages", "mo_messages")
      .groupBy("Date", "NatCo", "Agent", "type")
      .agg(sum("mo_messages").alias("MO_messages"),
        sum("mt_messages").alias("MT_messages"))
      .withColumn("MTMO_messages", col("mo_messages") + col("mt_messages"))

    val eventsByConvAllTypes = eventsByConvType.as("main")
      //.filter(col("type") =!= "single_message")
      .filter((col("type") === "a2p_conversation") or (col("type") === "p2a_conversation") )
      .join(d_agent, d_agent("Agent") === eventsByConvType("Agent"), "left")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .withColumn("TypeOfConvID",
        when(col("type") === "p2a_conversation", "2")
          .otherwise("1"))
      .select("Date","NatCoID","AgentID","TypeOfConvID","MO_messages", "MT_messages","MTMO_messages")

    eventsByConvAllTypes
  }

  override def process_F_Conversation_And_SM(rbm_billable_events: DataFrame, d_natco: DataFrame,
                                             d_agent: DataFrame):DataFrame = {
    logger.info("Processing f_conversation_and_sm for day")
    val eventsMessageAllTypes = rbm_billable_events
      .withColumn("Date", split(col("start_time"), "T").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .na.fill(0,Array("duration"))
      .withColumn("conversation",
        when((col("type") === "a2p_conversation") or (col("type") === "p2a_conversation"), lit(1)
        ).otherwise(lit(0))
      )
      .withColumn("single_message",
        when((col("type") === "single_message"), lit(1)
        ).otherwise(lit(0))
      )
      .withColumn("basic_message",
        when((col("type") === "basic_message"), lit(1)
        ).otherwise(lit(0))
      )
      .groupBy("Date", "NatCo", "Agent", "type")
      .agg(
        bround(avg("duration"),2).alias("AverageDuration"),
        sum("conversation").alias("NoOfConv"),
        sum("single_message").alias("NoOfSM"),
        sum("basic_message").alias("NoOfBM")
      )
      .withColumn("TypeOfConvID",
        when(col("type") === "p2a_conversation", lit("2"))
          .otherwise(lit("1")))

    val fact = eventsMessageAllTypes.as("main")
      .join(d_agent, d_agent("Agent") === eventsMessageAllTypes("Agent"), "left")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      //handle for a2p single messages which are not part of a conversation
      //.withColumn("NoOfConv",
       // when((col("TypeOfSM") === "single_message" or col("TypeOfSM") === "basic_message") && col("TypeOfConv").isNull, 0)
       //   .otherwise(col("NoOfConv")))
      //.withColumn("AverageDuration", regexp_replace(col("AverageDuration"), lit("\\."), lit(",")))
      .select("Date", "NatCoID", "AgentID", "TypeOfConvID", "AverageDuration", "NoOfConv", /*"TypeOfSM",*/"NoOfSM", "NoOfBM")
    //fact.show()
    fact
  }

  override def process_F_UAU_Daily(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame = {
    logger.info("Processing f_uau_daily")

    new_acc_uau_daily.as("main")
      .select("Date", "NatCo","AgentID" ,"user_id")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .withColumn("Date", col("Date").cast("date"))
      .groupBy("Date", "NatCoID", "AgentID")
      .agg(countDistinct("user_id").alias("UAU_daily"))
      .select("Date", "NatCoID", "AgentID","UAU_daily")
      .orderBy("Date")

  }

  override def process_F_UAU_Monthly(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame = {
    logger.info("Processing f_uau_monthly")

    new_acc_uau_daily.as("main")
      .select("Date", "NatCo", "AgentID","user_id")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .withColumn("YearMonth", date_format(col("Date"),"yyyy-MM"))
      .groupBy("YearMonth", "NatCoID", "AgentID")
      .agg(countDistinct("user_id").alias("UAU_monthly"))
      .select("YearMonth", "NatCoID","AgentID" ,"UAU_monthly")
  }

  override def process_F_UAU_Yearly(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame = {
    logger.info("Processing f_uau_yearly")

    new_acc_uau_daily.as("main")
      .select("Date", "NatCo","AgentID" ,"user_id")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .withColumn("Year", year(col("Date")))
      .groupBy("Year", "NatCoID", "AgentID")
      .agg(countDistinct("user_id").alias("UAU_yearly"))
      .select("Year", "NatCoID","AgentID" ,"UAU_yearly")
  }

  override def process_F_UAU_Total(new_acc_uau_daily:DataFrame, d_natco: DataFrame):DataFrame = {
    logger.info("Processing f_uau_total")

    new_acc_uau_daily.as("main")
      .select("Date", "NatCo","AgentID" ,"user_id")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .groupBy( "NatCoID", "AgentID")
      .agg(countDistinct("user_id").alias("UAU_total"))
      .select( "NatCoID","AgentID" ,"UAU_total")
  }
}
