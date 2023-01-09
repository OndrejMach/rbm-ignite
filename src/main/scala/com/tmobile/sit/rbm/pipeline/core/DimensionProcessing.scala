package com.tmobile.sit.rbm.pipeline.core

import com.tmobile.sit.rbm.pipeline.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit, row_number, split, when}
import org.apache.spark.sql.types.LongType

/**
 * Class trait/interface which needs to be implemented for creating dimensional data from
 * input and lookup data
 */
trait DimensionProcessing extends Logger {
  def process_D_Agent_Owner(rbm_billable_events: DataFrame): DataFrame

  def process_D_Agent(rbm_activity: DataFrame, rbm_billable_events: DataFrame, d_agent_owner: DataFrame): DataFrame

  def process_D_Content_Type(rbm_activity: DataFrame, ContentDescriptionMapping: DataFrame): (DataFrame,DataFrame)
}

class Dimension(implicit sparkSession: SparkSession) extends DimensionProcessing {

  override def process_D_Agent_Owner(rbm_billable_events: DataFrame): DataFrame = {
    logger.info("Processing d_agent_owner for today")

    rbm_billable_events
      .withColumn("AgentOwner", split(col("agent_owner"), "@").getItem(0))
      .select("AgentOwner")
      .distinct()
      .withColumn("AgentOwnerID", row_number.over(Window.orderBy("AgentOwner")))
      .select("AgentOwnerID", "AgentOwner") //Order columns
  }

  override def process_D_Agent(rbm_activity: DataFrame, rbm_billable_events: DataFrame, d_agent_owner: DataFrame): DataFrame = {
    logger.info("Processing d_agent for today")

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
      .withColumn("Agent", split(distinctAgents("agent_id"), "@").getItem(0))
      .withColumn("AgentOwner", split(col("agent_owner"), "@").getItem(0))
      .join(d_agent_owner, split(col("agent_owner"), "@").getItem(0) === d_agent_owner("AgentOwner"), "left")
      .select("AgentID", "AgentOwnerID", "Agent")
    //
  }

  override def process_D_Content_Type(rbm_activity: DataFrame, contentDescriptionMapping: DataFrame): (DataFrame, DataFrame) = {
    //contentDescriptionMapping.show(false)

    logger.info("Processing d_content_type for today")
    val max = contentDescriptionMapping.select(functions.max("ContentID").cast(LongType)).collect()(0).getLong(0)
    logger.info(s"Getting Max contentID ${max}")

    // Select distinct activity_id and type pairs
    val evaluated = rbm_activity.select("activity_id", "type").distinct()
      .join(contentDescriptionMapping,
        rbm_activity("type") === contentDescriptionMapping("OriginalContent"),
        "left")

    val window = Window.orderBy("ContentID")
    val newContentTypes = evaluated
      .filter(col("ContentID").isNull)
      .withColumn("ContentID", row_number().over(window) + max)

    val specified = evaluated.filter(col("ContentID").isNotNull)

    val overall = newContentTypes.union(specified)

    (overall.drop("type", "activity_id").distinct(),
      overall.select(col("ContentID"),col("type").as("OriginalContent"),col("Content"))
        .withColumn("Content",
          when(col("Content").isNotNull,
            col("Content"))
            .otherwise(lit("N/A"))
        ).union(contentDescriptionMapping.select("ContentID","OriginalContent","Content"))
        .distinct()
        .orderBy("ContentID")
    )


    //.withColumnRenamed("activity_id","ContentID")
  }
}
