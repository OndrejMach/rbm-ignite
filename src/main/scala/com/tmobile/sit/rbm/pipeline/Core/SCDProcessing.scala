package com.tmobile.sit.rbm.pipeline.Core

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, row_number}

/**
 * A trait defining preprocessing of the input data. There are two method for preprocessing of each data source.
 */

trait SCDProcessing extends Logger{
  def processD_AgentOwner(old_d_agent_owner: DataFrame, new_d_agent_owner: DataFrame) : DataFrame
  def processD_Agent(old_d_agent: DataFrame, new_d_agent: DataFrame) : DataFrame
  def processD_ContentType(old_d_content_type: DataFrame, new_d_content_type: DataFrame) : DataFrame

  //todo: implement generic method
  def processSCD(old_dimension: DataFrame, new_dimension: DataFrame): DataFrame
}

/**
 * Preprocessing implementation, two methods - one for people table preprocessing and one for salaryInfo preprocessing.
 */
class ProcessSCD(implicit sparkSession: SparkSession) extends SCDProcessing {
  import sparkSession.sqlContext.implicits._

  override def processD_AgentOwner(old_d_agent_owner: DataFrame, new_d_agent_owner: DataFrame): DataFrame = {
    old_d_agent_owner
    .withColumn("Order",lit("1"))
    .union(
      new_d_agent_owner
        .withColumn("Order",lit("2"))
        .as("new")
        .join(old_d_agent_owner.as("old"),
          $"old.AgentOwner" === $"new.AgentOwner",
          "leftouter")
        .filter($"old.AgentOwnerID".isNull)
        .select($"new.AgentOwnerID", $"new.AgentOwner", $"new.Order")
    )
    .as("merged")
    .withColumn("AgentOwnerID", row_number.over(Window.orderBy("merged.Order")))
    .select("AgentOwnerID","AgentOwner")
  }
  override def processD_Agent(old_d_agent: DataFrame, new_d_agent: DataFrame): DataFrame = {
    old_d_agent
      .withColumn("Order",lit("1"))
      .union(
        new_d_agent
          .withColumn("Order",lit("2"))
          .as("new")
          .join(old_d_agent.as("old"),
            $"old.Agent" === $"new.Agent",
            "leftouter")
          .filter($"old.AgentID".isNull)
          .select($"new.AgentID",$"new.AgentOwnerID", $"new.Agent", $"new.Order")
      )
      .as("merged")
      .withColumn("AgentID", row_number.over(Window.orderBy("merged.Order")))
      .select("AgentID","AgentOwnerID", "Agent")
  }
  override def processD_ContentType(old_d_content_type: DataFrame, new_d_content_type: DataFrame): DataFrame = {
    old_d_content_type
     .union(
        new_d_content_type
          .withColumn("Order",lit("2"))
          .as("new")
          .join(old_d_content_type.as("old"),
            $"old.ContentID" === $"new.ContentID",
            "leftouter")
          .filter($"old.OriginalContent".isNull)
          .select($"new.ContentID",$"new.OriginalContent", $"new.Content")
      )
      .as("merged")
      .select("ContentID","OriginalContent", "Content")
  }
  override def processSCD(old_dimension: DataFrame, new_dimension: DataFrame): DataFrame = {
    old_dimension
  }
}
