package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

/**
 * Definition of the case classes used for holding intermediary data stractures - input and preprocessed DataFrames
 * */

case class InputData(rbm_activity: Reader,
                     rbm_billable_events: Reader,
                     NatCoMapping: Reader,
                     ConversationTypeMapping: Reader,
                     ContentDescriptionMapping: Reader,
                     file_natco_id: String,
                     file_date: String)

case class PreprocessedData(rbm_activity: DataFrame,
                            rbm_billable_events: DataFrame,
                            NatCoMapping: DataFrame,
                            ConversationTypeMapping: DataFrame,
                            ContentDescriptionMapping: DataFrame)

case class ResultPaths(lookupPath: String, outputPath: String)

case class OutputData(d_natco: DataFrame,
                      d_content_type: DataFrame,
                      d_conversation_type: DataFrame,
                      d_agent: DataFrame,
                      d_agent_owner: DataFrame,
                      f_message_content: DataFrame,
                      f_conversations_and_sm: DataFrame,
                      f_message_conversation: DataFrame)
