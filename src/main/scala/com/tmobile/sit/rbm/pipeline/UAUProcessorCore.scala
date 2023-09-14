package com.tmobile.sit.rbm.pipeline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

trait UAUProcessorInterface  {
  def getUAUAggregates : DataFrame
}

class UAUProcessorCore(accUsersDaily: Reader, natcoMapping: Reader) extends UAUProcessorInterface with Logger {
  val getUAUAggregates: DataFrame  ={

    accUsersDaily.read()
      .join(natcoMapping.read(), Seq("NatCo"), "left_outer")
      .select("AgentID", "NatCoID", "Date", "user_id")
      .distinct()
      .sort(desc("Date"))


  }
}
