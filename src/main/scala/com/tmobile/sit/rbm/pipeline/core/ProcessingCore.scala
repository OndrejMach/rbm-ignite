package com.tmobile.sit.rbm.pipeline.core

import com.tmobile.sit.common.Logger
import com.tmobile.sit.rbm.data.{OutputData, PersistentData, PreprocessedData}
import com.tmobile.sit.rbm.pipeline.OutputData
import org.apache.spark.sql.SparkSession

/**
 * Class trait/interface which needs to be implemented
 */
trait ProcessingCore extends Logger {
  def process(preprocessedData: PreprocessedData, persistentData: PersistentData) : OutputData
}

/**
 * This class implements the core processing method which creates and updates dimensional data
 */
class CoreProcessing(implicit sparkSession: SparkSession) extends ProcessingCore {

  /**
   * The process method creates the output files as dimensions and facts
   */
  override def process(preprocessedData: PreprocessedData, persistentData: PersistentData): OutputData = {
    logger.info("Executing  processing core")

    //TODO: Find other workaround for false crossJoin detection besides enabling crosJoin in spark
    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")
    val handleSCD = new SCDHandler()
    val dimensionProcessor = new Dimension()
    val factProcessor = new Fact()

    logger.info("Creating static dimensions d_natco and d_conversation_type")
    // Static dimension mapping, always overwrite
    val d_natco = preprocessedData.NatCoMapping
    val d_conversation_type = preprocessedData.ConversationTypeMapping

    // Slowly changing dimensions
    //**********************
    val new_d_agent_owner = dimensionProcessor.process_D_Agent_Owner(preprocessedData.rbm_billable_events)
    val d_agent_owner = handleSCD.handle_D_Agent_Owner(persistentData.d_agent_owner, new_d_agent_owner)
    //**********************
    val new_d_agent = dimensionProcessor.process_D_Agent(preprocessedData.rbm_activity,preprocessedData.rbm_billable_events,d_agent_owner)
    val d_agent = handleSCD.handle_D_Agent(persistentData.d_agent, new_d_agent)
    //**********************
    val new_d_content_type = dimensionProcessor.process_D_Content_Type(preprocessedData.rbm_activity, preprocessedData.ContentDescriptionMapping)
    val d_content_type = handleSCD.handle_D_Content_Type(persistentData.d_content_type, new_d_content_type)

    // Daily fact tables, always overwrite suffixed with date and natco
    val f_message_content = factProcessor.process_F_Message_Content(preprocessedData.rbm_activity,
      d_natco,
      d_content_type,
      d_agent)
    val f_message_conversation = factProcessor.process_F_Message_Conversation(preprocessedData.rbm_billable_events,
      d_natco,
      d_agent)
    val f_conversations_and_sm = factProcessor.process_F_Conversation_And_SM(preprocessedData.rbm_billable_events,
      d_natco,
      d_agent)

    // UAU accumulating facts. The accumulator is already preprocessed
    val new_acc_users_daily = preprocessedData.AccUsersDaily

    val f_uau_daily = factProcessor.process_F_UAU_Daily(new_acc_users_daily, d_natco)
    val f_uau_monthly = factProcessor.process_F_UAU_Monthly(new_acc_users_daily, d_natco)
    val f_uau_yearly = factProcessor.process_F_UAU_Yearly(new_acc_users_daily, d_natco)
    val f_uau_total = factProcessor.process_F_UAU_Total(new_acc_users_daily, d_natco)

    //Return OutputData object
    OutputData(d_natco,
      d_content_type,
      d_conversation_type,
      d_agent,
      d_agent_owner,
      f_message_content,
      f_conversations_and_sm,
      f_message_conversation,
      f_uau_daily,
      f_uau_monthly,
      f_uau_yearly,
      f_uau_total,
      new_acc_users_daily)
  }
}