package com.tmobile.sit.jobtemplate.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

/**
 * A trait defining preprocessing of the input data. There are two method for preprocessing of each data source.
 */

trait TemplateStageProcessing extends Logger{
  def preprocessPeople(input: DataFrame) : DataFrame
  def preprocessSalaryInfo(input: DataFrame) : DataFrame
}

/**
 * Preprocessing implementation, two methods - one for people table preprocessing and one for salaryInfo preprocessing.
 */
class TemplateStage extends TemplateStageProcessing {
  /**
   * Prepares people data for processing. Basically a simple step dropping one column and filtering data based on the ID value.
   * @param people - a DataFrame containing people table read from CSV
   * @return - people data tuned and preprocessed.
   */
  override def preprocessPeople(people: DataFrame): DataFrame = {
    people.drop("notes")
      .filter("id <15")
  }

  /**
   * Preprocessing of the salaryInfo data. Selects only two columns needed and adds one more 'isValid' with a default value 'true'
   * @param salaryInfo - input salaryInfo table from csv.
   * @return - preprocessed data as DataFrame.
   */
  override def preprocessSalaryInfo(salaryInfo: DataFrame) : DataFrame = {
    salaryInfo
      .select("id", "salary")
      .withColumn("isValid", lit(true))
  }
}
