package com.codeslower.query

import java.nio.charset.Charset
import java.text.DecimalFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

trait Schema {
  def recordSize: Int
  def columns: List[String]
}

trait Row {
  def column(col : String): Option[String]
  def index: Int
  def schema: Schema
}

object StbSchema {
  private val revenueParser = new DecimalFormat("#,###.##")
  revenueParser.setParseBigDecimal(true)
  private val dateParser = new java.text.SimpleDateFormat("yyyy-MM-dd")

  lazy val instance = new StbSchema
  
  def parseRevenue(revenue: String): java.math.BigDecimal =
    Option(revenueParser.parse(revenue).asInstanceOf[java.math.BigDecimal]) match {
      case Some(r) => r
      case _ => throw new RuntimeException(s"Failed to parse revenue: $revenue")
    }

  def parseViewDate(date: String): Date = Option(dateParser.parse(date)) match {
    case Some(x) => x
    case _ => throw new RuntimeException(s"Failed to parse date: $date")
  }
  
  def parseViewTime(viewTime: String): Duration = {
    val v = viewTime.split(":")
    Duration(Integer.parseInt(v(0).trim) * 60 + Integer.parseInt(v(1).trim), TimeUnit.MINUTES)
  }
}

class StbSchema extends Schema {
  private lazy val _recordLength: Int = 212

  override def recordSize = _recordLength

  override def columns: List[String] = List("STB", "TITLE", "PROVIDER", "DATE", "REV", "VIEW_TIME")

  val stbpos = 0
  val stblen = 64

  val titlepos = stbpos + stblen
  val titlelen = 64

  val providerpos = titlepos + titlelen
  val providerlen = 64

  val datepos = providerpos + providerlen
  val datelen = 10

  val revpos = datepos + datelen
  val revlen = 5

  val viewpos = revpos + revlen
  val viewlen = 5

}

class StbIndexedRow(idx: Int, store: FileStore[StbIndexedRow]) extends Row {
  def column(col: String): Option[String] = row match {
    case Some(row) => row.column(col)
    case _ => None
  }

  def row: Option[StbRow] = store.getAt(idx) match {
    case Success(Some(row)) => Some(bytesToRow(row))
    case _ => None
  }

  override def schema = StbSchema.instance

  override def index: Int = idx

  private def bytesToRow(bytes: Array[Byte]): StbRow = {
    val utf8: Charset = Charset.forName("UTF-8")
    val stb = new String(bytes, schema.stbpos, schema.stblen, utf8).trim
    val title = new String(bytes, schema.titlepos, schema.titlelen, utf8).trim
    val provider = new String(bytes, schema.providerpos, schema.providerlen, utf8).trim
    val date = new String(bytes, schema.datepos, schema.datelen, utf8).trim
    val rev = new String(bytes, schema.revpos, schema.revlen, utf8).trim
    val view_time = new String(bytes, schema.viewpos, schema.viewlen, utf8).trim
    
    new StbRow(Option(stb), 
      Option(StbSchema.parseViewDate(date)),
      Option(StbSchema.parseViewTime(view_time)),
      Option(StbSchema.parseRevenue(rev)),
      Option(title),
      Option(provider), idx)
  }
}

case class StbRow(stb: Option[String],
             date: Option[Date],
             viewTime: Option[Duration],
             revenue: Option[java.math.BigDecimal],
             title: Option[String],
             provider: Option[String],
             idx: Int) extends Row {

  private val dateFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
  private val numberFormatter = new DecimalFormat("#,###.##")

  override def index: Int = idx

  override def schema: StbSchema = StbSchema.instance

  override def column(col : String): Option[String] = {
    col.toUpperCase.trim match {
      case "STB" => stb
      case "TITLE" => title
      case "PROVIDER" => provider
      case "DATE" => date.map(dateFormatter.format)
      case "REV" => revenue.map(numberFormatter.format)
      case "VIEW_TIME" => viewTime.map(t =>
        s"${t.toHours.formatted("%02d")}:${(t.toMinutes - t.toHours * 60).formatted("%02d")}")
    }
  }
}
