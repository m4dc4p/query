package com.codeslower.query

trait Select[A <: Row] extends Function1[Iterator[A], Iterator[A]] {
  def apply(rows: Iterator[A]) : Iterator[A]
}

class Projection(cols: List[String], row: Row) extends Row {
  override def columns: List[String] = cols

  override def column(col : String): Option[String] = {
    if(cols.contains(col.toUpperCase.trim))
      row.column(col)
    else
      None
  }
}

object Projection {
  def apply(cols: List[String], row: Row) = {
    new Projection(cols.map(_.toUpperCase.trim), row)
  }
}

object Select {
  def apply(cols : List[String]): Select[Row] = {
    cols match {
        // Empty list means all columns
      case Nil => new Select[Row] {
        override def apply(rows: Iterator[Row]): Iterator[Row] = rows
      }
      case _ => new Select[Row] {
        override def apply(rows: Iterator[Row]): Iterator[Row] = {
          for (r <- rows) yield Projection(cols, r)
        }
      }
    }
  }
}