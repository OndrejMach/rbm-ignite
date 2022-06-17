package com.tmobile.sit.rbm.pipeline

import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType


/**
 * Reading CSVs and returning dataframe. For better testability all readers implement trait Reader.
 * There are two versions of CSV readers - one reading a particular csv file and second capable of reading multiple CSVs
 * from a folder (CSVs must have the same schema of course).
 *
 * @author Ondrej Machacek
 */

/**
 * Abstract class to extract common characteristics of CSV file rearing in spark. Any new CSV reader alternative should inherit from this one.
 */
private[pipeline] abstract class CsvGenericReader extends Logger {
  def getCSVReader(header: Boolean,
                   badRecordsPath: Option[String] = None,
                   delimiter: String = ",",
                   quote: String = "\"",
                   escape: String = "\\",
                   encoding: String = "UTF-8",
                   schema: Option[StructType] = None,
                   timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
                   dateFormat: String = "yyyy-MM-dd",
                   multiline: Boolean = false
                  )(implicit sparkSession: SparkSession): DataFrameReader = {
    val reader = sparkSession
      .read
      .option("header", if (header) "true" else "false")
      .option("quote", quote)
      .option("escape", escape)
      .option("delimiter", delimiter)
      .option("encoding", encoding)
      .option("timestampFormat", timestampFormat)
      .option("dateFormat", dateFormat)
      .option("multiLine", multiline)

    val invalidHandling = if (badRecordsPath.isDefined) {
      logger.info(s"Bad records to be stored in ${badRecordsPath.get}")
      reader.option("badRecordsPath", badRecordsPath.get)
    }
    else reader.option("mode", "DROPMALFORMED")

    if (schema.isDefined) {
      invalidHandling.schema(schema.get)
    } else {
      logger.warn("Schema file not defined, trying to infer one")
      invalidHandling.option("inferschema", "true")
    }
  }
}

/**
 * Basic CSV reader reading a single file, returning dataframe.
 *
 * @param path            path to the file
 * @param header          indicates wheter file contains a header on the first line.
 * @param badRecordsPath  path to the folder where invalid records will be stored - not used when None - default.
 * @param delimiter       you can specify here a delimiter char. by default ',' is used.
 * @param quote           character used for quoting, by default it's '"'
 * @param escape          escape character to input specila chars - by default '\'
 * @param encoding        test encoding - by default UTF-8
 * @param schema          using spark types you can define Typed schema - it's highly recommended to us this parameter. By default schema is inferred.
 * @param timestampFormat format for timestamp data
 * @param dateFormat      format for date data
 * @param sparkSession    implicit SparkSession used for reading.
 */
class CSVReader(path: String,
                header: Boolean,
                badRecordsPath: Option[String] = None,
                delimiter: String = ",",
                quote: String = "\"",
                escape: String = "\\",
                encoding: String = "UTF-8",
                schema: Option[StructType] = None,
                timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
                dateFormat: String = "yyyy-MM-dd",
                multiline: Boolean = false
               ) (implicit sparkSession: SparkSession) extends CsvGenericReader with Reader {

  private def getCsvData(path: String): DataFrame = {
    logger.info(s"Reading CSV from path ${path}")
    val reader = getCSVReader(
      header, badRecordsPath, delimiter,
      quote, escape, encoding,
      schema, timestampFormat, dateFormat, multiline
    )
    reader.csv(path)

  }

  override def read(): DataFrame = getCsvData(path)
}

/**
 * A little enhanced CSV reader capable of reading multiple csv files from a single path. Result is a single dataframe (union-ed data from each CSV).
 * CSV files must have the same structure.
 *
 * @param path            path to the file
 * @param fileList        list of filenames to be read from the path
 * @param header          indicates wheter file contains a header on the first line.
 * @param badRecordsPath  path to the folder where invalid records will be stored - not used when None - default.
 * @param delimiter       you can specify here a delimiter char. by default ',' is used.
 * @param quote           character used for quoting, by default it's '"'
 * @param escape          escape character to input specila chars - by default '\'
 * @param encoding        test encoding - by default UTF-8
 * @param schema          using spark types you can define Typed schema - it's highly recommended to us this parameter. By default schema is inferred.
 * @param timestampFormat format for timestamp data
 * @param dateFormat      format for date data
 * @param sparkSession    implicit SparkSession used for reading.
 */
class CSVMultifileReader(path: String, fileList: Seq[String],
                         header: Boolean,
                         badRecordsPath: Option[String] = None,
                         delimiter: String = ",",
                         quote: String = "\"",
                         escape: String = "\\",
                         encoding: String = "UTF-8",
                         schema: Option[StructType] = None,
                         timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
                         dateFormat: String = "yyyy-MM-dd",
                         multiline: Boolean = false
                        )
                        (implicit sparkSession: SparkSession) extends CsvGenericReader with Reader {
  private def getCsvData(path: String): DataFrame = {
    logger.info(s"Reading CSV from path ${path}")
    val reader = getCSVReader(
      header, badRecordsPath, delimiter,
      quote, escape, encoding,
      schema, timestampFormat, dateFormat,multiline
    )
    reader.csv(fileList.map(path + "/" + _): _*)

  }

  override def read(): DataFrame = getCsvData(path)
}

/**
 * Companion object for the simple CSV reader
 */

object CSVReader {
  def apply(path: String,
            header: Boolean,
            badRecordsPath: Option[String] = None,
            delimiter: String = ",",
            quote: String = "\"",
            escape: String = "\\",
            encoding: String = "UTF-8",
            schema: Option[StructType] = None,
            timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
            dateFormat: String = "yyyy-MM-dd",
            multiline: Boolean = false
           )(implicit sparkSession: SparkSession): CSVReader =

    new CSVReader(
      path, header, badRecordsPath,
      delimiter, quote, escape,
      encoding, schema, timestampFormat,
      dateFormat, multiline
    )(sparkSession)
}

/**
 * Companion object for the multifile CSV reader
 */
object CSVMultifileReader {
  def apply(path: String,
            fileList: Seq[String],
            header: Boolean,
            badRecordsPath: Option[String] = None,
            delimiter: String = ",",
            quote: String = "\"",
            escape: String = "\\",
            encoding: String = "UTF-8",
            schema: Option[StructType] = None,
            timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
            dateFormat: String = "yyyy-MM-dd",
            multiline: Boolean = false
           )
           (implicit sparkSession: SparkSession): CSVMultifileReader =

    new CSVMultifileReader(
      path, fileList, header,
      badRecordsPath, delimiter, quote,
      escape, encoding, schema,
      timestampFormat, dateFormat, multiline
    )(sparkSession)
}