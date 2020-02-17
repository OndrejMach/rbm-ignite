package com.tmobile.sit.jobtemplate.pipeline

import com.tmobile.sit.common.readers.Reader
import org.apache.spark.sql.DataFrame

case class InputData(people: Reader, salaryInfo: Reader)

case class PreprocessedData(peopleData: DataFrame, salaryData: DataFrame)
