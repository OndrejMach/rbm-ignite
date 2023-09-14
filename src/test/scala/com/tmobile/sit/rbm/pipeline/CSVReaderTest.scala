package com.tmobile.sit.rbm.pipeline

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import com.tmobile.sit.rbm.data.{InputData, InputTypes}
import org.apache.spark.sql.SparkSession

@RunWith(classOf[JUnitRunner])
class CSVReaderTest extends FlatSpec with DatasetSuiteBase with Matchers {

  implicit lazy val _: SparkSession = spark

  import spark.implicits._

  it should ("read ") in {

    val inputReaders = InputData(
      rbm_activity = new CSVReader("src/test/resources/input/rbm_activity_2023-08-30.csv_st.csv.gz",
                                    header = true, delimiter = "\t", schema = Some(InputTypes.activityType)),
      rbm_billable_events = null)

      val activities = inputReaders.rbm_activity.read()
      activities.show(false)
    1 should be (1)
  }


}
