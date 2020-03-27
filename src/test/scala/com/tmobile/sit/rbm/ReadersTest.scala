package com.tmobile.sit.rbm

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.tmobile.sit.common.readers.{CSVMultifileReader, CSVReader, ExcelReader, MSAccessReader}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReadersTest extends FlatSpec with DataFrameSuiteBase {
  implicit lazy val _: SparkSession = spark

  "csvReaderPeople" should "read csv with header" in {
    import spark.implicits._

    val csvReader = CSVReader("src/main/resources/inputData/people.csv", true)

    val df = csvReader.read().filter("name = 'jarda'")
    val refDF = ReferenceData.people_csv_with_header.toDF
    //df.printSchema()
    //refDF.printSchema()

    assertDataFrameEquals(df, refDF) // equal
    /*
    val input2 = List(4, 5, 6).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(input1, input2) // not equal
    }

 */

  }
}