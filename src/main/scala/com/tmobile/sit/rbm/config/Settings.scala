package com.tmobile.sit.rbm.config


import com.tmobile.sit.common.config.GenericSettings

/**
 * Case class used as to hold job parameters.
 * @param inputPath - input folder
 * @param lookupPath - lookup folder
 * @param outputPath - output folder
 * @param appName - spark application name on the cluster for easier identification
 */

case class Settings(inputPath: Option[String]
                    , lookupPath: Option[String]
                    , outputPath: Option[String]
                    , appName: Option[String]
                   ) extends GenericSettings
{
  /**
   * simple check whether all the parameters are defined
   * @return true if all the parameters defined properly, false otherwise
   */
  def isAllDefined: Boolean = {
    this.inputPath.isDefined && this.inputPath.get.nonEmpty &&
      this.lookupPath.isDefined && this.lookupPath.get.nonEmpty &&
      this.outputPath.isDefined && this.outputPath.get.nonEmpty &&
      this.appName.isDefined && this.appName.get.nonEmpty
  }
}
