package com.tmobile.sit.rbm.data

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

/**
 * Definition of the case classes used for holding data structures
 * */

case class InputData(rbm_activity: Reader,
                     rbm_billable_events: Reader)

case class MappingData(NatCoMapping: Reader,
                       ConversationTypeMapping: Reader,
                       ContentDescriptionMapping: Reader)

case class FileMetaData(file_natco_id: String,
                        file_date: String)

case class PersistentData(d_agent: DataFrame,
                          d_agent_owner: DataFrame,
                          d_content_type: DataFrame,
                          acc_users_daily: DataFrame)

case class PreprocessedData(rbm_activity: DataFrame,
                            rbm_billable_events: DataFrame,
                            NatCoMapping: DataFrame,
                            ConversationTypeMapping: DataFrame,
                            ContentDescriptionMapping: DataFrame,
                            AccUsersDaily: DataFrame)

case class ResultPaths(lookupPath: String, outputPath: String)

case class OutputData(d_natco: DataFrame,
                      d_content_type: DataFrame,
                      d_conversation_type: DataFrame,
                      d_agent: DataFrame,
                      d_agent_owner: DataFrame,
                      f_message_content: DataFrame,
                      f_conversations_and_sm: DataFrame,
                      f_message_conversation: DataFrame,
                      f_uau_daily: DataFrame,
                      f_uau_monthly: DataFrame,
                      f_uau_yearly: DataFrame,
                      f_uau_total: DataFrame,
                      new_acc_uau_daily: DataFrame)
