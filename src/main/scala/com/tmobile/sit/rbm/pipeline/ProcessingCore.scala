package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.functions.{col, split, row_number, count, sum, when}
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
  import sparkSession.sqlContext.implicits._

  def getContentMapping(rbm_activity: DataFrame): DataFrame  = {
    // Select distinct activity_id and type pairs
    rbm_activity.select("activity_id", "type").distinct()
  }

  def getAgentMapping(rbm_activity: DataFrame, rbm_billable_events: DataFrame): DataFrame = {
    val windowSpec  = Window.partitionBy("Agent").orderBy("Agent")

    rbm_activity.select("agent_id")
      .union(rbm_billable_events.select("agent_id"))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .drop("agent_id")
      .distinct()
      .withColumn("AgentID", row_number.over(Window.orderBy("Agent")))
  }

  def getMessagesByType(rbm_activity: DataFrame, NatCoMapping: DataFrame,
                        ContentMapping: DataFrame, AgentMapping: DataFrame): DataFrame = {

     val activityGrouped = rbm_activity
      .withColumn("Date", split(col("time"), " ").getItem(0))
      .withColumn("Agent", split(col("agent_id"), "@").getItem(0))
      .select("Date", "NatCo", "Agent", "type", "Agent", "direction")
      .groupBy("Date", "NatCo", "Agent", "type", "Agent","direction")
      .agg(count("type").alias("Count"))

    val ActivitiesWithMessageTypes = activityGrouped
      .groupBy("Date", "NatCo", "Agent", "type")
      .agg(sum("Count").alias("MTMO_MessagesByType"))
      .join(activityGrouped.filter(col("direction") === "mt"),
        Seq("Date", "NatCo", "Agent", "type"), "left")
      .withColumn("MT_MessagesByType", when(col("Count").isNull, 0).otherwise(col("Count")))
      .drop("direction", "Count")
      .join(activityGrouped.filter(col("direction") === "mo"),
        Seq("Date", "NatCo", "Agent", "type"), "left")
      .withColumn("MO_MessagesByType", when(col("Count").isNull, 0).otherwise(col("Count")))
      .drop("direction", "Count")

    ActivitiesWithMessageTypes.createOrReplaceTempView("ActivitiesWithMessageTypes")
    NatCoMapping.createOrReplaceTempView("NatCoMapping")
    ContentMapping.createOrReplaceTempView("ContentMapping")
    AgentMapping.createOrReplaceTempView("AgentMapping")

    sparkSession.sql("select * from ActivitiesWithMessageTypes a " +
      "left join AgentMapping ag " +
      "on a.Agent == ag.Agent " +
      "left join ContentMapping c " +
      "on a.type == c.type ")
      .drop("Agent", "type")

      //.join(NatCoMapping, activitiesWithMessageTypes("NatCo") === NatCoMapping("NatCo"), "left")
      //.join(ContentMapping, activityGrouped("type") === ContentMapping("type"), "left")
      //.join(AgentMapping, Seq("Agent"), "left")

  }

  /**
   * TODO: Implement core processing class methods to deal with data
   */
  override def process(preprocessedData: PreprocessedData): OutputData = {
    logger.info("Executing  processing core")

    val ContentMapping = getContentMapping(preprocessedData.rbm_activity)
    val AgentMapping = getAgentMapping(preprocessedData.rbm_activity,preprocessedData.rbm_billable_events)
    val MessagesByType = getMessagesByType(preprocessedData.rbm_activity, preprocessedData.NatCoMapping, ContentMapping, AgentMapping)

    MessagesByType.show()

    OutputData(preprocessedData.NatCoMapping, ContentMapping, AgentMapping)
  }
}