package com.tmobile.sit.jobtemplate.pipeline

import com.tmobile.sit.common.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

trait TemplateStageProcessing extends Logger{
  def preprocessPeople(input: DataFrame) : DataFrame
  def preprocessSalaryInfo(input: DataFrame) : DataFrame
}


class TemplateStage extends TemplateStageProcessing {
  override def preprocessPeople(people: DataFrame): DataFrame = {
    people.drop("notes")
      .filter("id <15")
  }
  override def preprocessSalaryInfo(salaryInfo: DataFrame) : DataFrame = {
    salaryInfo
      .select("id", "salary")
      .withColumn("isValid", lit(true))
  }
}
