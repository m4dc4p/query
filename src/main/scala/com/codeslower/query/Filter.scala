package com.codeslower.query

import java.math.BigDecimal
import java.text.{NumberFormat, DecimalFormat}
import java.util.Date

import scala.concurrent.duration.Duration

object Filter {
  private def makePredicate(col : String, value : String): StbRow => Boolean = col.toUpperCase.trim match {
    case "STB" => { _.stb.exists(_.equals(value)) }
    case "TITLE" => { _.title.exists(_.equals(value)) }
    case "PROVIDER" => { _.provider.exists(_.equals(value)) }
    case "DATE" => {
      val viewDate: Date = StbSchema.parseViewDate(value)
      _.date.exists(_.equals(viewDate))
    }
    case "REV" => {
      val revenue: BigDecimal = StbSchema.parseRevenue(value)
      _.revenue.exists(_.compareTo(revenue) == 0)
    }
    case "VIEW_TIME" => {
      val view_date: Duration = StbSchema.parseViewTime(value)
      _.viewTime.exists(_.equals(view_date)) }
  }

  def makeFilters(ls : List[(String, String)], filter : Option[StbRow] => Option[StbRow]): StbRow => Boolean = ls match {
    case Nil => { row: StbRow => filter(Some(row)).exists(_ => true) }
    case (col, value) :: rest => {
      val matcher = makePredicate(col, value)
      makeFilters(rest, filter.compose((optRow: Option[StbRow]) =>
        for(row <- optRow if matcher(row))
          yield row ))
    }
  }

  def apply(filters : List[(String, String)]) : Filter[StbIndexedRow] = {
    val predicate = makeFilters(filters, { x => x })
    new Filter[StbIndexedRow] {
      override def apply(rows: Iterator[StbIndexedRow]): Iterator[StbIndexedRow] = {
        for (r <- rows;
             row <- r.row if predicate(row))
          yield r
      }
    }
  }
}

trait Filter[A <: Row] extends Function1[Iterator[A], Iterator[A]] {
  def apply(rows : Iterator[A]) : Iterator[A]
}