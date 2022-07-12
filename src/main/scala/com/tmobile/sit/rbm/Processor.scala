package com.tmobile.sit.rbm

import com.tmobile.sit.rbm.config.Setup
import com.tmobile.sit.rbm.data.{FileMetaData, InputData, MappingData, PersistentData, ResultPaths}
import com.tmobile.sit.rbm.pipeline.core.CoreProcessing
import com.tmobile.sit.rbm.pipeline.output.ResultWriter
import com.tmobile.sit.rbm.pipeline.stage.Stage
import com.tmobile.sit.rbm.pipeline.Pipeline
import com.tmobile.sit.rbm.pipeline.Logger
import com.tmobile.sit.rbm.pipeline.CSVReader
import org.apache.spark.sql.SparkSession

object Processor extends App with Logger {
  logger.info("Started processing")

  // Check arguments
  if(args.length == 0){
    logger.error("Arguments required. Options: -natco=<natco> [-date=<date yyyy-mm-dd>]")
    System.exit(1)
  }

  var natco_arg = new Object
  var date_arg = new Object

  // Parse and validate arguments
  for(arg<-args) {
    if(arg.split("=").length == 0){
      logger.error("Incorrect argument format. Options: -natco=<natco> [-date=<date>] ")
    }
    else{
      if(arg.split("=")(0).equals("-natco")) {
          natco_arg = arg.split("=")(1)
        logger.info("natco=" + natco_arg)
        }
      else if(arg.split("=")(0).equals("-date")) {
        date_arg = arg.split("=")(1)
        logger.info("date=" + date_arg)
      }
    }
  }

  //Set config file based on system OS property
  val configFile = if(System.getProperty("os.name").startsWith("Windows")) {
    logger.info("Detected Windows configuration")
    "rbm_config.windows.conf"
  } else if (System.getProperty("os.name").startsWith("Mac")) {
    logger.info("Detected Mac configuration")
    "rbm_config.OM.conf"
  } else {
    logger.info("Detected Mac configuration")
    "rbm_config.linux.conf"
  }

  val conf = new Setup(configFile)

  if (!conf.settings.isAllDefined) {
    logger.error("Application not properly configured!!")
    conf.settings.printMissingFields()
    System.exit(1)
  }

  conf.settings.printAllFields()

  implicit val sparkSession: SparkSession = getSparkSession(conf.settings.appName.get, conf.settings.master.get)

  // The Web UI runs as long as the spark processing runs and is avaialble via the following URL
  logger.info("Web UI: " + sparkSession.sparkContext.uiWebUrl)

  // Prepare data structures
  val inputReaders = InputData(
    rbm_activity = new CSVReader(conf.settings.inputPath.get + s"/rbm_activity_${date_arg}.csv_${natco_arg}.csv.gz", header = true, delimiter = "\t"),
    rbm_billable_events = new CSVReader(conf.settings.inputPath.get + s"/rbm_billable_events_${date_arg}.csv_${natco_arg}.csv.gz", header = true, delimiter = "\t")
  )

  val fileMetaData = FileMetaData(
    file_date = date_arg.toString,
    file_natco_id = natco_arg.toString
  )

  //Set config file based on system OS property
  val mappingPath = if(System.getProperty("os.name").startsWith("Windows")) {
    logger.info("Using Windows project resources for static mapping")
    "src/main/resources/inputData/"
  } else {
    logger.info("Using Linux HDFS path for static mapping")
    conf.settings.lookupPath.get
  }

  val mappingReaders = MappingData(
    NatCoMapping =  new CSVReader(s"${mappingPath}NatCoMapping.csv", header = true, delimiter = ";"),
    ConversationTypeMapping =  new CSVReader(s"${mappingPath}ConvTypeMapping.csv", header = true, delimiter = ";"),
    ContentDescriptionMapping =  new CSVReader(s"${mappingPath}ContentDescriptionMapping.csv", header = true, delimiter = ";")
  )

  val persistentData = PersistentData(
    d_agent_owner = new CSVReader(conf.settings.outputPath.get + "d_agent_owner.csv", header = true, delimiter = ";").read(),
    d_agent = new CSVReader(conf.settings.outputPath.get + "d_agent.csv", header = true, delimiter = ";").read(),
    d_content_type = new CSVReader(conf.settings.outputPath.get + "d_content_type.csv", header = true, delimiter = ";").read(),
    f_conversations_and_sm = new CSVReader(conf.settings.outputPath.get + s"f_conversations_and_sm_${fileMetaData.file_natco_id}.csv", header = true, delimiter = ";").read(),
    f_message_content = new CSVReader(conf.settings.outputPath.get + s"f_message_content_${fileMetaData.file_natco_id}.csv", header = true, delimiter = ";").read(),
    f_message_conversation = new CSVReader(conf.settings.outputPath.get + s"f_message_conversation_${fileMetaData.file_natco_id}.csv", header = true, delimiter = ";").read(),
    acc_users_daily = new CSVReader(conf.settings.lookupPath.get + s"acc_users_daily_${fileMetaData.file_natco_id}.csv", header = true, delimiter = ";").read()
  )

  // Prepare processing blocks
  val stage = new Stage()

  val processingCore = new CoreProcessing()

  val resultPaths = ResultPaths(conf.settings.lookupPath.get, conf.settings.outputPath.get)

  val resultWriter = new ResultWriter(resultPaths, fileMetaData)

  // Prepare orchestration pipeline
  val pipeline = new Pipeline(inputReaders,mappingReaders,fileMetaData,persistentData,stage,processingCore,resultWriter)

  // Run and finish
  pipeline.run()

  logger.info("Processing finished")

}
