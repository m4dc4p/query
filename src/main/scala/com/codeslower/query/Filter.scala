package com.codeslower.query

import java.text.{NumberFormat, DecimalFormat}
import java.util.Date

object Filter {
  val revenueParser = new DecimalFormat("#,###.##")
  revenueParser.setParseBigDecimal(true)
  val dateParser = new java.text.SimpleDateFormat("yyyy-MM-dd")

  private def makePredicate(col : String, value : String): StbRow => Boolean = col.toUpperCase.trim match {
    case "STB" => { _.stb.exists(_.equals(value)) }
    case "TITLE" => { _.title.exists(_.equals(value)) }
    case "PROVIDER" => { _.provider.exists(_.equals(value)) }
    case "DATE" => {
      val date : Date = dateParser.parse(value)
      _.date.exists(_.equals(date))
    }
    case "REV" => {
      val rev = revenueParser.parse(value).asInstanceOf[java.math.BigDecimal]
      _.revenue.exists(_.equals(rev))
    }
    case "VIEW_TIME" => { _.viewTime.exists(_.equals(value)) }
  }

  def makeFilters(ls : List[(String, String)], filter : Option[StbRow] => Option[StbRow]): StbRow => Boolean = ls match {
    case Nil => { row : StbRow => filter(Some(row)).exists(_ => true) }
    case (col, value) :: rest => {
      val matcher = makePredicate(col, value)
      makeFilters(rest, filter.compose({ optRow : Option[StbRow] => for(row <- optRow; if matcher(row)) yield row }))
    }
  }

  def apply(filters : List[(String, String)]) : Filter[StbIndexedRow] = {
    val predicate = makeFilters(filters, { x => x })
    new Filter[StbIndexedRow] {
      override def apply(rows: Iterator[StbIndexedRow]): Iterator[StbIndexedRow] = {
        for (r <- rows; row <- r.row if predicate(row)) yield r
      }
    }
  }
}

trait Filter[A <: Row] extends Function1[Iterator[A], Iterator[A]] {
  def apply(rows : Iterator[A]) : Iterator[A]
}