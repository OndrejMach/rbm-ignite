package com.tmobile.sit.rbm.pipeline

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

/**
 * Definition of the case classes used for holding intermediary data stractures - input and preprocessed DataFrames
 * */

case class InputData(rbm_activity: Reader, rbm_billable_events: Reader)

case class PreprocessedData(rbm_activity: DataFrame, rbm_billable_events: DataFrame)

case class ResultPaths(lookupPath: String, outputPath: String)

case class OutputData(rbmFact: DataFrame)
