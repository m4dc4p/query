package com.codeslower.query

import java.text.DecimalFormat
import java.util.Date

import scala.concurrent.duration.Duration

trait Row {
  def column(col : String): Option[String]
  def columns: List[String]
}

case class StbRow(stb: Option[String],
             date: Option[Date],
             viewTime: Option[Duration],
             revenue: Option[java.math.BigDecimal],
             title: Option[String],
             provider: Option[String]) extends Row {

  val dateFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val numberFormatter = new DecimalFormat("#,###.##")

  override def column(col : String): Option[String] = {
    col.toUpperCase.trim match {
      case "STB" => stb
      case "TITLE" => title
      case "PROVIDER" => provider
      case "DATE" => date.map(dateFormatter.format)
      case "REV" => revenue.map(dateFormatter.format)
      case "VIEW_TIME" => viewTime.map(t => s"${t.toHours.formatted("%02d")}:${(t.toMinutes - t.toHours * 60).formatted("%02d")}")
    }
  }

  def columns = List("STB", "TITLE", "PROVIDER", "DATE", "REV", "VIEW_TIME")
}
