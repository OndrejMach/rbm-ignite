package com.tmobile.sit.rbm.pipeline

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

private[pipeline] abstract class Merger extends Logger {
  def copyMerge(
                 srcFS: FileSystem, srcDir: Path,
                 dstFS: FileSystem, dstFile: Path,
                 deleteSource: Boolean, conf: Configuration
               ): Boolean = {

    if (dstFS.exists(dstFile))
      dstFS.delete(dstFile, true)

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {

      val outputFile = dstFS.create(dstFile)
      Try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile() =>
              val inputFile = srcFS.open(status.getPath())
              Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
              inputFile.close()
          }
      }
      outputFile.close()

      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }



  def merge(srcPath: String, dstPath: String): Unit = {
    logger.info(s"Merging spark output ${srcPath} into a single file ${dstPath}")
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    hdfs.delete(new Path(dstPath), true)
    //FileUtil.fullyDelete(new File(dstPath))
    copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig)
  }
}

/**
 * CSV Writer class. An instance is able to write CSV files according to the class parameters. It implements method writaData which writes
 * input DataFrame. Writer by default creates just one single partition and creates a CSV file, not folder as regular Spark csv writer does.
 * @author Ondrej Machacek
 *
 * @param path - path and filename for the resulting file. Its a regular file, not folder!
 * @param mergeToSingleFile - if true a single csv file is created - no folder (as spark does it) is there - default is true
 * @param delimiter - delimiter used in the file
 * @param writeHeader - if true header is written as the first line
 * @param quote - quoting character
 * @param escape - used escape character
 * @param encoding - file text encoding
 * @param quoteMode - what is encoded basically. Read spark csv writer documentation for details.
 * @param sparkSession - implicit SparkSession for writing.
 */

class CSVWriter(data: DataFrame,
                path: String, mergeToSingleFile: Boolean = true,
                delimiter: String = ",", writeHeader: Boolean = true,
                quote: String = "\"", escape: String = "\\",
                encoding: String = "UTF-8", quoteMode: String = "MINIMAL",
                timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
                dateFormat: String = "yyyy-MM-dd", nullValue: String = "", quoteAll: String = "false", emptyValue: String = null)(implicit sparkSession: SparkSession) extends Merger with Writer {


  def writeData() : Unit = {
    logger.info(s"Writing data to ${path} " )
    data
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", if (writeHeader) "true" else "false")
      .option("sep", delimiter)
      .option("quote", quote )
      .option("escape", escape)
      .option("quoteMode",quoteMode )
      .option("encoding", encoding)
      .option("timestampFormat", timestampFormat)
      .option("dateFormat", dateFormat)
      .option("nullValue", nullValue)
      .option("quoteAll",quoteAll )
      .option("emptyValue", emptyValue)
      .csv(path+"_tmp")

    if (mergeToSingleFile) merge(path+"_tmp", path)
  }
}

object CSVWriter {
  def apply(data: DataFrame,
            path: String, mergeToSingleFile: Boolean = true,
            delimiter: String = ",",
            writeHeader: Boolean = true,
            quote: String = "\"",
            escape: String = "\\",
            encoding: String = "UTF-8",
            quoteMode: String = "MINIMAL",
            timestampFormat: String = "MM/dd/yyyy HH:mm:ss.SSSZZ",
            dateFormat: String = "yyyy-MM-dd", nullValue: String = "", quoteAll: String = "false", emptyValue: String = null)
           (implicit sparkSession: SparkSession): CSVWriter =

    new CSVWriter(data,path,mergeToSingleFile ,delimiter, writeHeader, quote, escape, encoding, quoteMode, timestampFormat, dateFormat, nullValue, quoteAll, emptyValue)(sparkSession)
}

