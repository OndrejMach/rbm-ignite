package com.tmobile.sit.jobtemplate.pipeline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

/**
 * trait defining interface for the main processing block
 */

trait ProcessingCore {
  def process(preprocessedData: PreprocessedData) : DataFrame
}

/**
 * This class implements core of the processing, for each important processing steps there is a method (two here for join and final aggregation).
 * This split should help with readability and structure. Core of the processing is method process which is basically ran first.
 */
class CoreLogicWithTransform extends ProcessingCore {
  /**
   * A method defining Join step in the code of the processing. It joins preprocessed People Dataframe and salaryInfo dataframe. Returns a Dataframe.
   *
   * @param salaryInfo - salaryInfo Dataframe
   * @param people - People DataFrame specified as a second parameter in a way to support spark's transform function for better chaining.
   * @return resulting joined DataFrame.
   */
  private def joinPeopleAndSalaryInfo(salaryInfo: DataFrame)(people: DataFrame) = {
    people.join(salaryInfo,Seq("id"), "inner")
  }

  /**
   * A method defining second processing stage - aggregation. It takes joined DataFrame (people + salaryInfo) and creates an desired aggregate based on
   * salary and address.
   *
   * @param peopleWithSalary - people joined with salaryInfo created by the previous step.
   * @return - resulting aggregated DataFrame
   */
  private def aggregateOnSalary(peopleWithSalary: DataFrame) : DataFrame = {
    peopleWithSalary
      .groupBy("salary", "address")
      .agg(count("salary")
        .alias("count"))
  }

  /**
   * The first method executing Join and aggregate steps in the desired order. This approach uses spark's tranform DataFrame method for cleaner orchestration.
   * @param preprocessedData - a case class containing preprocessed input data - people and their salary information
   * @return - aggregated input as DataFrame
   */
  override def process(preprocessedData: PreprocessedData): DataFrame = {
    preprocessedData.peopleData
      .transform(joinPeopleAndSalaryInfo(preprocessedData.salaryData))
      .transform(aggregateOnSalary)
  }
}

/**
 * This class performs the same processing as the class above - CoreLogicWithTransform. For method chaining it uses standard Scala way how to
 * take a Sequence of functions and executing them one by one. See the description above for the details regarding the join and aggregate methods.
 */

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

  /**
   * The main method executing the processing. Here steps are orchestrated using scala chain functionality where Seq of functions is executed in order.
   * @param preprocessedData - a case class containing preprocessed data.
   * @return - final aggregates as DataFrame
   */
  override def process(preprocessedData: PreprocessedData): DataFrame = {

    val pipeline = Function.chain(List(joinPeopleAndSalaryInfo(preprocessedData.salaryData)(_), aggregateOnSalary(_)))

    preprocessedData.peopleData
      .transform(pipeline)
  }
}