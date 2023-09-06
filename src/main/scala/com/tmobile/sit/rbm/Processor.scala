package com.tmobile.sit.rbm

import com.tmobile.sit.rbm.data.{FileMetaData, InputData, InputTypes, MappingData, PersistentData, ResultPaths}
import com.tmobile.sit.rbm.pipeline.core.CoreProcessing
import com.tmobile.sit.rbm.pipeline.output.ResultWriter
import com.tmobile.sit.rbm.pipeline.stage.Stage
import com.tmobile.sit.rbm.pipeline.Pipeline
import com.tmobile.sit.rbm.pipeline.Logger
import com.tmobile.sit.rbm.pipeline.CSVReader
import org.apache.spark.sql.SparkSession



object Processor extends App with Logger {
  //billing_event_id        type    agent_id        agent_owner
  // billing_party   max_duration_single_message
  // max_duration_a2p_conversation        max_duration_p2a_conversation
  // start_time      duration        mt_messages     mo_messages
  // size_kilobytes  agent_name      owner_name
  logger.info("Started processing")

  // Check arguments
  if(args.length == 0){
    logger.error("Arguments required. Options: -natco=<natco> [-date=<date yyyy-mm-dd>] | uau ")
    System.exit(1)
  }

  var natco_arg = ""
  var date_arg = ""

  // Parse and validate arguments
  for(arg<-args) {
    if(arg.split("=").length == 0){
      logger.error("Incorrect argument format. Options: -natco=<natco> [-date=<date>] | uau")
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

  val conf = getConfig()

  implicit val sparkSession: SparkSession = getSparkSession(conf.settings.appName.get, conf.settings.master.get)

  // The Web UI runs as long as the spark processing runs and is avaialble via the following URL
  logger.info("Web UI: " + sparkSession.sparkContext.uiWebUrl)

  // Prepare data structures
  val inputReaders = InputData(
    rbm_activity = new CSVReader(conf.settings.inputPath.get + s"/rbm_activity_${date_arg}.csv_${natco_arg}.csv.gz", header = true, delimiter = "\t", schema = Some(InputTypes.activityType)),
    rbm_billable_events = new CSVReader(conf.settings.inputPath.get + s"/rbm_billable_events_${date_arg}.csv_${natco_arg}.csv.gz", header = true, delimiter = "\t", schema = Some(InputTypes.billableType))
  )
  if (inputReaders.rbm_activity.read().count() ==0 && inputReaders.rbm_billable_events == 0){
    sparkSession.stop()
    logger.info("No data to process")
    System.exit(0)
  }

  val fileMetaData = FileMetaData(
    file_date = date_arg,
    file_natco_id = natco_arg
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
