package com.codeslower.query

trait Select[Input <: Row] extends Function1[Iterator[Input], Iterator[Row]] {
  def apply(rows: Iterator[Input]) : Iterator[Row]
}

class Projection(cols: List[String], row: Row) extends Row {

  override def index: Int = row.index

  override def schema: Schema = new Schema {
    override def columns: List[String] = cols

    override def recordSize: Int = 1
  }

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
  def apply[Input <: Row](cols : List[String]): Select[Input] = {
    cols match {
        // Empty list means all columns
      case Nil => new Select[Input] {
        override def apply(rows: Iterator[Input]): Iterator[Row] = rows
      }
      case _ => new Select[Input] {
        override def apply(rows: Iterator[Input]): Iterator[Row] = {
          for (r <- rows) yield Projection(cols, r)
        }
      }
    }
  }
}