package com.tmobile.sit.jobtemplate.config

import com.tmobile.sit.common.config.GenericSettings

case class Settings(inputPathPeople: Option[String]
                    , inputPathSalaryInfo: Option[String]
                    , outputPath: Option[String]
                    , appName: Option[String]
                   ) extends GenericSettings
{

  def isAllDefined: Boolean = {
    this.inputPathPeople.isDefined && this.inputPathPeople.get.nonEmpty &&
      this.inputPathSalaryInfo.isDefined && this.inputPathSalaryInfo.get.nonEmpty &&
      this.outputPath.isDefined && this.outputPath.get.nonEmpty &&
      this.appName.isDefined && this.appName.get.nonEmpty
  }
}
