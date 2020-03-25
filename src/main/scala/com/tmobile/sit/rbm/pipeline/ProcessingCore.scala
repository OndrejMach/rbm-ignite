package com.tmobile.sit.rbm.pipeline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

/**
 * trait defining interface for the main processing block
 */

trait ProcessingCore {
  def process(preprocessedData: PreprocessedData) : OutputData
}

/**
 * This class implements core of the processing, for each important processing steps there is a method (two here for join and final aggregation).
 * This split should help with readability and structure. Core of the processing is method process which is basically ran first.
 */
class CoreLogicWithTransform extends ProcessingCore {

  // TODO
  /**
   * Implement core processing class
   */
  override def process(preprocessedData: PreprocessedData): OutputData = {
    OutputData(null)
  }
}