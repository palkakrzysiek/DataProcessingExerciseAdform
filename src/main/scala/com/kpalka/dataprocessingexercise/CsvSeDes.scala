package com.kpalka.dataprocessingexercise

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object CsvSeDes {
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss.SSS")
  def deserializeView(entry: Map[String, String]): View =
    View(
      id = entry("id").toLong,
      logTime = LocalDateTime.parse(entry("logtime"), dateFormatter),
      campaignId = entry("campaignid").toLong
    )
  def deserializeClick(entry: Map[String, String]): Click =
    Click(
      id = entry("id").toLong,
      logTime = LocalDateTime.parse(entry("logtime"), dateFormatter),
      campaignId = entry("campaignid").toLong,
      interactionId = entry("interactionid").toLong
    )
  def serializeViewWithClick(v: ViewWithClick): String = s"${v.clickId},${v.logTime.format(dateFormatter)},${v.clickId}"
}
