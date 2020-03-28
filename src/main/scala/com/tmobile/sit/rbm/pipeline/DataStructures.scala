package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

/**
 * Definition of the case classes used for holding intermediary data stractures - input and preprocessed DataFrames
 * */

case class InputData(rbm_activity: Reader,
                     rbm_billable_events: Reader,
                     NatCoMapping: Reader,
                     file_natco_id: String,
                     file_date: String)

case class PreprocessedData(rbm_activity: DataFrame,
                            rbm_billable_events: DataFrame,
                            NatCoMapping: DataFrame)

case class ResultPaths(lookupPath: String, outputPath: String)

case class OutputData(NatCoMapping: DataFrame,
                      ContentMapping: DataFrame,
                      AgentMapping: DataFrame,
                      MessagesByType: DataFrame,
                      NoOfConvAndSM: DataFrame,
                      NoOfMessByTypeOfConv: DataFrame)
