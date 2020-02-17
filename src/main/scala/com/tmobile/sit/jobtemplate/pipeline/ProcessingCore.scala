package com.tmobile.sit.jobtemplate.pipeline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

trait ProcessingCore {
  def process(preprocessedData: PreprocessedData) : DataFrame
}

class CoreLogicWithTransform extends ProcessingCore {
  private def joinPeopleAndSalaryInfo(salaryInfo: DataFrame)(people: DataFrame) = {
    people.join(salaryInfo,Seq("id"), "inner")
  }
  private def aggregateOnSalary(peopleWithSalary: DataFrame) : DataFrame = {
    peopleWithSalary
      .groupBy("salary", "address")
      .agg(count("salary")
        .alias("count"))
  }


  override def process(preprocessedData: PreprocessedData): DataFrame = {
    preprocessedData.peopleData
      .transform(joinPeopleAndSalaryInfo(preprocessedData.salaryData))
      .transform(aggregateOnSalary)
  }
}

class CoreLogicWithChain extends ProcessingCore {
  private def joinPeopleAndSalaryInfo(salaryInfo: DataFrame)(people: DataFrame) = {
    people.join(salaryInfo,Seq("id"), "inner")
  }
  private def aggregateOnSalary(peopleWithSalary: DataFrame) : DataFrame = {
    peopleWithSalary
      .groupBy("salary", "address")
      .agg(count("salary")
        .alias("count"))
  }

  override def process(preprocessedData: PreprocessedData): DataFrame = {

    val pipeline = Function.chain(List(joinPeopleAndSalaryInfo(preprocessedData.salaryData)(_), aggregateOnSalary(_)))

    preprocessedData.peopleData
      .transform(pipeline)
  }
}