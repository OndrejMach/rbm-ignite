package com.tmobile.sit.rbm.pipeline.core

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, row_number, sum}

/**
 * Class trait/interface which needs to be implemented for merging old dimensional
 * data with new data calcualted from the current day's values
 */
trait SCDProcessing extends Logger{
  def handle_D_Agent_Owner(old_d_agent_owner: DataFrame, new_d_agent_owner: DataFrame) : DataFrame
  def handle_D_Agent(old_d_agent: DataFrame, new_d_agent: DataFrame) : DataFrame
  def handle_D_Content_Type(old_d_content_type: DataFrame, new_d_content_type: DataFrame) : DataFrame
}

class SCDHandler(implicit sparkSession: SparkSession) extends SCDProcessing {
  import sparkSession.sqlContext.implicits._

  override def handle_D_Agent_Owner(old_d_agent_owner: DataFrame, new_d_agent_owner: DataFrame): DataFrame = {
    logger.info("Handling d_agent_owner SCD")

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

  override def handle_D_Agent(old_d_agent: DataFrame, new_d_agent: DataFrame): DataFrame = {
    logger.info("Handling d_agent SCD")

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

  override def handle_D_Content_Type(old_d_content_type: DataFrame, new_d_content_type: DataFrame): DataFrame = {
    logger.info("Handling d_content_type SCD")

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
}


