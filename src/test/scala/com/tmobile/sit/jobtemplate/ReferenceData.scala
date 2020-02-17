package com.tmobile.sit.jobtemplate

final case class InputRowCSV(id: Option[Int], name: Option[String], address: Option[String],notes: Option[String])


object ReferenceData {
  //1,jarda,"Praha", "nic asi" id,name,address,notes
  def  people_csv_with_header = List( InputRowCSV(Some(1),Some("jarda"),Some("Praha"), Some("nic asi")))


}
