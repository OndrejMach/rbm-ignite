package com.tmobile.sit.rbm.pipeline.core

import com.tmobile.sit.rbm.pipeline.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{avg, bround, col, date_format, lit, max, regexp_replace, row_number, sum}
import org.apache.spark.sql.types.{DoubleType, LongType}

/**
 * Class trait/interface which needs to be implemented for merging old dimensional
 * data with new data calcualted from the current day's values
 */
trait SCDHandling extends Logger{
  def handle_D_Agent_Owner(old_d_agent_owner:DataFrame,new_d_agent_owner:DataFrame):DataFrame
  def handle_D_Agent(old_d_agent:DataFrame,new_d_agent:DataFrame):DataFrame
  def handle_D_Content_Type(old_d_content_type:DataFrame,new_d_content_type:DataFrame):DataFrame
}

class SCDHandler(implicit sparkSession: SparkSession) extends SCDHandling {
  import sparkSession.sqlContext.implicits._

  override def handle_D_Agent_Owner(old_d_agent_owner:DataFrame,new_d_agent_owner:DataFrame):DataFrame = {
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

  override def handle_D_Agent(old_d_agent:DataFrame,new_d_agent:DataFrame):DataFrame = {
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

  override def handle_D_Content_Type(old_d_content_type:DataFrame,new_d_content_type:DataFrame):DataFrame = {
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
      .distinct()
  }
}

/**
 * Class trait/interface which needs to be implemented for merging previous
 * fact data with new data calcualted from the current day's values
 */
trait FactPersistence extends Logger {
  def handle_Accumulating_Fact(old_fact:DataFrame,new_fact:DataFrame,file_date:String):DataFrame
}

class FactHandler(implicit sparkSession: SparkSession) extends FactPersistence{

  override def handle_Accumulating_Fact(old_fact:DataFrame,new_fact:DataFrame,file_date:String):DataFrame = {
    handleDate(old_fact,new_fact, file_date)
      .withColumn("AverageDuration",regexp_replace(col("AverageDuration"), lit(","), lit("\\.")).cast(DoubleType) )
      .groupBy("Date", "NatCoID", "AgentID", "TypeOfConvID")
      .agg(bround(avg("AverageDuration"), 2).as("AverageDuration"),
        sum("NoOfConv").cast(LongType).as("NoOfConv"),
        sum("NoOfSM").cast(LongType).as("NoOfSM"),
        sum("NoOfBM").cast(LongType).as("NoOfBM")
      )
      .withColumn("AverageDuration", regexp_replace(col("AverageDuration"), lit("\\."), lit(",")))
      .orderBy("Date")
      .select("Date", "NatCoID", "AgentID", "TypeOfConvID", "AverageDuration", "NoOfConv", /*"TypeOfSM",*/"NoOfSM", "NoOfBM")

    //Return old fact plus new fact
    /*old_fact
      .withColumn("Date", col("Date").cast("date"))
      .filter(col("Date") =!= file_date)
      .union(new_fact)
      .orderBy("Date")

     */
  }
  private def handleDate(old_fact: DataFrame,new_fact: DataFrame, file_date: String): DataFrame = {
    old_fact
      .withColumn("Date", col("Date").cast("date"))
      //.withColumn("Date", date_format(col("Date"), "yyyy-MM-dd"))
      .filter(col("Date") =!= lit(file_date))
       .union(
        new_fact
          .withColumn("Date", col("Date").cast("date"))
          //.withColumn("Date", date_format(col("Date"), "yyyy-MM-dd"))
      )

  }

  def handle_F_message_conversation(old_fact: DataFrame, new_fact: DataFrame, file_date: String): DataFrame = {
    handleDate(old_fact, new_fact, file_date)
      .groupBy("Date", "NatCoID", "AgentID", "TypeOfConvID")
      .agg(sum("MO_messages").cast(LongType).as("MO_messages"),
        sum("MT_messages").cast(LongType).as("MT_messages"),
        sum("MTMO_messages").cast(LongType).as("MTMO_messages"))
      .orderBy("Date")
      .select("Date","NatCoID","AgentID","TypeOfConvID","MO_messages", "MT_messages","MTMO_messages")
  }
  // Exception for F_Message_Content where we need to group and sum values
  def handle_F_Message_Content(old_fact:DataFrame,new_fact:DataFrame,file_date:String):DataFrame = {
    handleDate(old_fact,new_fact,file_date)
      .groupBy("Date", "NatCoID", "ContentID", "AgentID")
      .agg(sum("MT_MessagesByType").cast(LongType).as("MT_MessagesByType"),
        sum("MO_MessagesByType").cast(LongType).as("MO_MessagesByType"),
        sum("MTMO_MessagesByType").cast(LongType).as("MTMO_MessagesByType"))
      .orderBy("Date")
  }
}
