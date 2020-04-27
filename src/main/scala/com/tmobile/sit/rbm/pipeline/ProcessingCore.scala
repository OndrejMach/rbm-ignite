package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.Logger
import com.tmobile.sit.rbm.pipeline.core.{Dimension, Fact, SCDHandler}
import org.apache.spark.sql.{SparkSession}

/**
 * trait defining interface for the main processing block
 */

trait ProcessingCore extends Logger {
  def process(preprocessedData: PreprocessedData, persistentData: PersistentData) : OutputData
}

/**
 * This class implements the core processing method which creates and updates dimensional data
 */
class CoreLogicWithTransform (implicit sparkSession: SparkSession) extends ProcessingCore {

  /**
   * The process class creates the output files as dimensions and facts
   */
  override def process(preprocessedData: PreprocessedData, persistentData: PersistentData): OutputData = {
    logger.info("Executing  processing core")

    //TODO: Find other workaround for false crossJoin detection besides enabling crosJoin in spark
    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")
    val handleSCD = new SCDHandler()
    val dimensionProcessor = new Dimension()
    val factProcessor = new Fact()

    // Static dimension mapping, always overwrite
    val d_natco = preprocessedData.NatCoMapping
    val d_conversation_type = preprocessedData.ConversationTypeMapping

    // Slowly changing dimensions
    //**********************
    val old_d_agent_owner = persistentData.d_agent_owner
    val new_d_agent_owner = dimensionProcessor.process_D_Agent_Owner(preprocessedData.rbm_billable_events)
    val d_agent_owner = handleSCD.handle_D_Agent_Owner(old_d_agent_owner, new_d_agent_owner)
    //**********************
    val new_d_agent = dimensionProcessor.process_D_Agent(preprocessedData.rbm_activity,preprocessedData.rbm_billable_events,d_agent_owner)
    val old_d_agent = persistentData.d_agent
    val d_agent = handleSCD.handle_D_Agent(old_d_agent, new_d_agent)
    //**********************
    val new_d_content_type = dimensionProcessor.process_D_Content_Type(preprocessedData.rbm_activity, preprocessedData.ContentDescriptionMapping)
    val old_d_content_type = persistentData.d_content_type
    val d_content_type = handleSCD.handle_D_Content_Type(old_d_content_type, new_d_content_type)

    // Daily fact tables, always overwrite suffixed with date and natco
    val f_message_content = factProcessor.process_F_Message_Content(preprocessedData.rbm_activity,
      d_natco, /*Not actually used because of compilation bug. Used static mapping instead.*/
      d_content_type,
      d_agent)
    val f_message_conversation = factProcessor.process_F_Message_Conversation(preprocessedData.rbm_billable_events,
      d_natco, /*Not actually used because of compilation bug. Used static mapping instead.*/
      d_agent)
    val f_conversations_and_sm = factProcessor.process_F_Conversation_And_SM(preprocessedData.rbm_billable_events,
      d_natco, /*Not actually used because of compilation bug. Used static mapping instead.*/
      d_agent)
    val f_uau = factProcessor.process_F_UAU(preprocessedData.rbm_activity, d_natco /*Not actually used. Used static mapping instead.*/)

    //Return OutputData object
    OutputData(d_natco,
      d_content_type,
      d_conversation_type,
      d_agent,
      d_agent_owner,
      f_message_content,
      f_conversations_and_sm,
      f_message_conversation,
      f_uau)
  }
}