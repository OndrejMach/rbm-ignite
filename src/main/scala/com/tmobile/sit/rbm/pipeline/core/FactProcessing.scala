package com.tmobile.sit.rbm.pipeline.core

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, concat_ws, count, countDistinct, lit, month, regexp_replace, row_number, split, sum, when, year}

trait FactProcessing extends Logger{
  def process_F_Message_Content(rbm_activity: DataFrame, d_natco: DataFrame,
                                d_content_type: DataFrame, d_agent: DataFrame): DataFrame

  def process_F_Message_Conversation(rbm_billable_events: DataFrame,
    d_natco: DataFrame, d_agent: DataFrame):DataFrame

  def process_F_Conversation_And_SM(rbm_billable_events: DataFrame, d_natco: DataFrame,
                                    d_agent: DataFrame):DataFrame

  def process_F_UAU(rbm_activity: DataFrame, d_natco: DataFrame):DataFrame

}


class Fact(implicit sparkSession: SparkSession) extends FactProcessing {
  import sparkSession.sqlContext.implicits._

  override def process_F_Message_Content(rbm_activity: DataFrame, d_natco: DataFrame,
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
    activitiesGrouped_AllDirections.as("main")
      .join(d_content_type, d_content_type("OriginalContent") === activitiesGrouped_AllDirections("type"), "left")
      .join(d_agent, d_agent("Agent") === activitiesGrouped_AllDirections("Agent"), "left")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      .select("Date", "NatCoID", "ContentID", "AgentID", "MT_MessagesByType", "MO_MessagesByType", "MTMO_MessagesByType")
  }

  override def process_F_Message_Conversation(rbm_billable_events: DataFrame,
                                              d_natco: DataFrame,
                                              d_agent: DataFrame):DataFrame = {

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

    eventsMessageAllTypes.as("main")
      .join(d_agent, d_agent("Agent") === eventsMessageAllTypes("Agent"), "left")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
      //fix for a2p single messages which are not part of a conversation
      .withColumn("NoOfConv",
        when(col("TypeOfSM") === "single_message" && col("TypeOfConv").isNull, 0)
          .otherwise(col("NoOfConv")))
      .withColumn("AverageDuration", regexp_replace(col("AverageDuration"), lit("\\."), lit(",")))
      .select("Date", "NatCoID", "AgentID", "TypeOfConvID", "AverageDuration", "NoOfConv", /*"TypeOfSM",*/"NoOfSM")
  }

  override def process_F_UAU(rbm_activity: DataFrame, d_natco: DataFrame):DataFrame = {

    import sparkSession.implicits._

    val rbm_acivity_YMD = rbm_activity.as("main")
      .join(d_natco.as("lookup"),$"main.NatCo" === $"lookup.NatCo", "left")
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
}
