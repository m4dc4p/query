package com.codeslower

import java.math
import java.util.{Calendar, Date}
import java.util.concurrent.TimeUnit

import com.codeslower.query._
import org.scalatest._

import scala.concurrent.duration.Duration
import scala.util.{Success, Failure, Try}

class QueryTest extends FlatSpec {

    private val inputFile = new FileStore[StbIndexedRow] {
      val rows: Array[StbRow] = Array(
        new StbRow(stb=Some("stb1"), date=Some(StbSchema.parseViewDate("2014-04-01")),
          viewTime=Some(StbSchema.parseViewTime("1:30")), revenue=Some(StbSchema.parseRevenue("4.00")),
          title=Some("the matrix"), provider=Some("warner bros"), idx=0),

        new StbRow(stb=Some("stb1"), title=Some("unbreakable"),
          provider=Some("buena vista"), date=Some(StbSchema.parseViewDate("2014-04-03")),
          revenue=Some(StbSchema.parseRevenue("6.00")),
          viewTime=Some(StbSchema.parseViewTime("2:05")), idx=1),

        new StbRow(stb=Some("stb2"), title=Some("the hobbit"),
          provider=Some("warner bros"), date=Some(StbSchema.parseViewDate("2014-04-02")),
          revenue=Some(StbSchema.parseRevenue("8.00")), viewTime=Some(StbSchema.parseViewTime("2:45")), idx=2),

        new StbRow(stb=Some("stb3"), title=Some("the matrix"), provider=Some("warner bros"),
          date=Some(StbSchema.parseViewDate("2014-04-02")),
          revenue=Some(StbSchema.parseRevenue("4.00")), viewTime=Some(StbSchema.parseViewTime("1:05")), idx=3),

        new StbRow(stb=None, title=None, provider=None, date=None, revenue=None, viewTime=None, idx=4)
      )


      override def schema: Schema = {
        StbSchema.instance
      }

      override def hasAt(idx: Int): Try[Option[Unit]] = {
        getAt(idx) match {
          case Success(Some(_)) => Success(Some(Unit))
          case Success(_) => Success(None)
          case Failure(t) => Failure(t)
        }
      }

      override def getAt(idx: Int): Try[Option[Array[Byte]]] = {
        idx match {
          case _ if idx >= 0 && idx < rows.length => Success(Some(new Array[Byte](0)))
          case _ => Failure(new RuntimeException(s"No row at $idx"))
        }
      }

      override def iterator: Iterator[StbIndexedRow] = {
        for (idx <- 0.to(rows.length - 1).toIterator)
        yield {
          new StbIndexedRow(idx, this) {
            override def row: Option[StbRow] = {
              Some(rows(idx))
            }
          }
        }
      }
    }

  "Order" should "sort by all columns correctly" in {
    for(column <- StbSchema.instance.columns) {
      val expected = column match {
        case "STB" => inputFile.rows.sorted(Ordering[String].on({ r: StbRow => r.stb.getOrElse("")}))
        case "TITLE" => inputFile.rows.sorted(Ordering[String].on({ r: StbRow => r.title.getOrElse("")}))
        case "PROVIDER" => inputFile.rows.sorted(Ordering[String].on({ r: StbRow => r.provider.getOrElse("")}))
        case "DATE" => inputFile.rows.sorted(Ordering[Date].on({ r: StbRow => r.date.getOrElse(new Date(0)) }))
        case "REV" => inputFile.rows.sorted(Ordering[java.math.BigDecimal].on({ r: StbRow => r.revenue.getOrElse(java.math.BigDecimal.ZERO)}))
        case "VIEW_TIME" => inputFile.rows.sorted(Ordering[Duration].on({ r: StbRow => r.viewTime.getOrElse(Duration(0, TimeUnit.SECONDS))}))
      }
      val result = Order(List(column)).apply(query.Filter(List()).apply(inputFile.iterator)).toList

      for ((r, e) <- result.zip(expected)) {
        assert(r.row.get == e, s"When sorting on $column.")
      }
    }
  }

  "Filter" should "filter empty rows; not filter matching rows" in {
    val expected = inputFile.rows.filter(r => r.stb.isDefined && r.stb.get == "stb1")
    val result = query.Filter(List(("stb", "stb1"))).apply(inputFile.iterator).toList
    assert(result.size == expected.size)

    for((r, e) <- result.zip(expected)) {
      assert(r.row.get == e, "No filters.")
    }
  }

  it should "filter all rows with no matches" in {
    val expected = inputFile.rows.filter(r => r.stb.isDefined && r.stb.get == "fajadlsfjskfja")
    val result = query.Filter(List(("stb", "fajadlsfjskfja"))).apply(inputFile.iterator).toList
    assert(result.size == expected.size)
  }

  it should "work with view_time" in {
    val expected = inputFile.rows.filter(r => r.viewTime.isDefined && r.viewTime.get == Duration(90, TimeUnit.MINUTES))
    val result = query.Filter(List(("view_time", "01:30"))).apply(inputFile.iterator).toList
    assert(result.size == expected.size)

    for((r, e) <- result.zip(expected)) {
      assert(r.row.get == e, "filter view_time.")
    }
  }

  it should "work with date" in {
    val expected = inputFile.rows.filter((r:StbRow) =>
      r.date.isDefined && r.date.get == new java.util.Date(114, 3, 1))
    val result = query.Filter(List(("date", "2014-04-01"))).apply(inputFile.iterator).toList
    assert(result.size == expected.size)

    for((r, e) <- result.zip(expected)) {
      assert(r.row.get == e, "filter date.")
    }
  }

  it should "work with revenue" in {
    val expected = inputFile.rows.filter((r:StbRow) =>
      r.revenue.isDefined && r.revenue.get.compareTo(java.math.BigDecimal.valueOf(4)) == 0)
    val result = query.Filter(List(("rev", "4.00"))).apply(inputFile.iterator).toList
    assert(result.size == expected.size)

    for((r, e) <- result.zip(expected)) {
      assert(r.row.get == e, "filter revenue.")
    }
  }

  it should "leave input untouched without any filters" in {
    val result = query.Filter(List()).apply(inputFile.iterator).toList
    val expected = inputFile.rows

    assert(result.size == expected.size)
    for((r, e) <- result.zip(expected)) {
      assert(r.row.get == e, "No filters.")
    }
  }


  it should "compare big decimal correctly" in {
    val filter = query.Filter(List(("rev", "1000")))
    val result = filter.apply(inputFile.iterator).toList
    val expected = inputFile.rows.filter(r => r.revenue.isDefined && r.revenue.get == java.math.BigDecimal.valueOf(1000))

    assert(result.size == expected.size)
    for((r, e) <- result.zip(expected)) {
      assert(r.row.get == e, "No filters.")
    }
  }

  "Select" should "only allow selected columns" in {
    val result = Select(List("stb", "date", "rev")).apply(inputFile.iterator).toList
    assert(result(0).schema.columns.map(_.toLowerCase.trim) == List("stb", "date", "rev"))
  }

  "Order" should "work on many columns" in {
    {
      val expected = inputFile.rows.sorted(
        Ordering[(String, java.math.BigDecimal)].on({r: StbRow =>
          (r.stb.getOrElse(""), r.revenue.getOrElse(java.math.BigDecimal.ZERO))})).toList
      val result = Order(List("stb", "rev")).apply(inputFile.iterator).toList

      for ((r, e) <- result.zip(expected)) {
        assert(r.row.get == e)
      }
    }

    {
      val expected = inputFile.rows.sorted(
        Ordering[(java.math.BigDecimal, String)].on({r: StbRow =>
          (r.revenue.getOrElse(java.math.BigDecimal.ZERO), r.stb.getOrElse(""))})).toList
      val result = Order(List("rev", "stb")).apply(inputFile.iterator).toList
      for ((r, e) <- result.zip(expected)) {
        assert(r.row.get == e)
      }
    }
  }
}
