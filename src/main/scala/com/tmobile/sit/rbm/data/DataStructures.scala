package com.tmobile.sit.rbm.data

import com.tmobile.sit.rbm.pipeline.Reader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ LongType, StringType, StructField}

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
                          f_conversations_and_sm: DataFrame,
                          f_message_content: DataFrame,
                          f_message_conversation: DataFrame,
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
                      new_acc_uau_daily: DataFrame,
                      newContentMapping: DataFrame
                     )
object InputTypes {
  val billableType = StructType(Array(
    StructField("billing_event_id", StringType, true),
    StructField("type", StringType, true),
    StructField("agent_id", StringType, true),
    StructField("agent_owner", StringType, true),
    StructField("billing_party", StringType, true),
    StructField("max_duration_single_message", LongType, true),
    StructField("max_duration_a2p_conversation", LongType, true),
    StructField("max_duration_p2a_conversation", LongType, true),
    StructField("start_time", StringType, true),
    StructField("duration", LongType, true),
    StructField("mt_messages", LongType, true),
    StructField("mo_messages", LongType, true),
    StructField("size_kilobytes", LongType, true),
    StructField("agent_name", StringType, true),
    StructField("owner_name", StringType, true)
  ))
  //activity_id     billing_event_id        agent_id        user_id direction       time    type    size_bytes
  val activityType = StructType(Array(
    StructField("activity_id", StringType, true),
    StructField("billing_event_id", StringType, true),
    StructField("agent_id", StringType, true),
    StructField("user_id", StringType, true),
    StructField("direction", StringType, true),
    StructField("time", StringType, true),
    StructField("type", StringType, true),
    StructField("size_bytes", LongType, true)
  ))
}