package com.tmobile.sit.jobtemplate.pipeline

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

/**
 * Definition of the case classes used for holding intermediary data stractures - input and preprocessed DataFrames
 * */

case class InputData(people: Reader, salaryInfo: Reader)

case class PreprocessedData(peopleData: DataFrame, salaryData: DataFrame)
