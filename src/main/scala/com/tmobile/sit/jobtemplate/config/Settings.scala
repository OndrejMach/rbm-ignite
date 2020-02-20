package com.tmobile.sit.jobtemplate.config

import com.tmobile.sit.common.config.GenericSettings

/**
 * Case class used as to hold job parameters.
 * @param inputPathPeople - input path for the people CSV file
 * @param inputPathSalaryInfo - input path for the salary CSV file - there is a reference ID to the people csv file
 * @param outputPath - path where output file is written
 * @param appName - spark application name on the cluster for easier identification
 */

case class Settings(inputPathPeople: Option[String]
                    , inputPathSalaryInfo: Option[String]
                    , outputPath: Option[String]
                    , appName: Option[String]
                   ) extends GenericSettings
{
  /**
   * simple check whether all the parameters are defined
   * @return true if all the parameters defined properly, false otherwise
   */
  def isAllDefined: Boolean = {
    this.inputPathPeople.isDefined && this.inputPathPeople.get.nonEmpty &&
      this.inputPathSalaryInfo.isDefined && this.inputPathSalaryInfo.get.nonEmpty &&
      this.outputPath.isDefined && this.outputPath.get.nonEmpty &&
      this.appName.isDefined && this.appName.get.nonEmpty
  }
}
