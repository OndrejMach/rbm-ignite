package com.tmobile.sit.rbm.pipeline.core

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, row_number, split}

/**
 * A trait defining preprocessing of the input data. There are two method for preprocessing of each data source.
 */

trait DimensionProcessing extends Logger{
  def process_D_Agent_Owner(rbm_billable_events: DataFrame):DataFrame
  def process_D_Agent(rbm_activity: DataFrame, rbm_billable_events: DataFrame, d_agent_owner: DataFrame): DataFrame
  def process_D_Content_Type(rbm_activity: DataFrame, ContentDescriptionMapping: DataFrame): DataFrame
}

/**
 * Preprocessing implementation, two methods - one for people table preprocessing and one for salaryInfo preprocessing.
 */
class Dimension(implicit sparkSession: SparkSession) extends DimensionProcessing {
  import sparkSession.sqlContext.implicits._

  override def process_D_Agent_Owner(rbm_billable_events: DataFrame):DataFrame = {
    rbm_billable_events
      .withColumn("AgentOwner", split(col("agent_owner"), "@").getItem(0))
      .select("AgentOwner")
      .distinct()
      .withColumn("AgentOwnerID", row_number.over(Window.orderBy("AgentOwner")))
      .select("AgentOwnerID", "AgentOwner") //Order columns
}

  override def process_D_Agent(rbm_activity: DataFrame, rbm_billable_events: DataFrame, d_agent_owner: DataFrame): DataFrame = {

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

  override def process_D_Content_Type(rbm_activity: DataFrame, ContentDescriptionMapping: DataFrame): DataFrame  = {
    // Select distinct activity_id and type pairs
    rbm_activity.select("activity_id", "type").distinct()
      .join(ContentDescriptionMapping,
        rbm_activity("type") === ContentDescriptionMapping("OriginalContent"),
        "left")
      .drop("type")
      .withColumnRenamed("activity_id","ContentID")
  }
}