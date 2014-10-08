package com.codeslower

import java.math
import java.util.Date

import org.scalatest._

import scala.concurrent.duration.Duration

class QueryTest extends FlatSpec {
  import com.codeslower.query._

  "Filter" should "filter empty rows" in {
    val rows = Seq(StbRow(None, None, None, None, None, None))
    val filter = Filter(List(("stb", "foo")))
    val result = filter.apply(rows.iterator).toSeq
    assert(result == Seq())
  }

  "Filter" should "not filter matching rows" in {
    val rows = Seq(StbRow(Some("foo"), None, None, None, None, None))
    val filter = Filter(List(("stb", "foo")))
    val result = filter.apply(rows.iterator).toSeq
    assert(result == rows)
  }


  it should "leave input untouched without any filters" in {
    {
      val rows = Seq(StbRow(Some("foo"), None, None, None, None, None))
      val filter = Filter(List())
      val result = filter.apply(rows.iterator).toSeq
      assert(result == rows)
    }

    {
      val rows = Seq(StbRow(None, None, None, None, None, None))
      val filter = Filter(List())
      val result = filter.apply(rows.iterator).toSeq
      assert(result == rows)
    }
  }

  it should "compare big decimal correctly" in {
    val expectedRow: StbRow = StbRow(Some("foo"), None, None, Some(math.BigDecimal.valueOf(1000)), None, None)
    val rows = Seq(expectedRow, StbRow(Some("bar"), None, None, Some(math.BigDecimal.valueOf(100)), None, None))
    val filter = Filter(List(("rev", "1000")))
    val result = filter.apply(rows.iterator).toSeq
    assert(result.size == 1)
    assert(result(0) == expectedRow)
  }

  "Select" should "only allow selected columns" in {
    val expectedRow: StbRow = StbRow(Some("foo"), Some(new Date), None, None, None, None)
    val rows = Seq(expectedRow)
    val select = Select(List("stb", "date", "rev"))
    val result = select.apply(rows.iterator).toSeq
    assert(result.size == 1)
    assert(result(0).columns.map(_.toLowerCase.trim) == List("stb", "date", "rev"))
    assert(result(0).column("stb") == expectedRow.column("stb"))
    assert(result(0).column("date") == expectedRow.column("date"))
    assert(result(0).column("rev") == expectedRow.column("rev"))
    assert(expectedRow.column("provider").isEmpty)
  }

  "Order" should "work on many columns" in {
    val rows = Seq(StbRow(Some("a"), None, None, Some(math.BigDecimal.valueOf(103)), None, None),
      StbRow(Some("a"), None, None, Some(math.BigDecimal.valueOf(102)), None, None),
      StbRow(Some("b"), None, None, Some(math.BigDecimal.valueOf(101)), None, None),
      StbRow(Some("b"), None, None, Some(math.BigDecimal.valueOf(100)), None, None))

    {
      val expectedRows = Seq(StbRow(Some("a"), None, None, Some(math.BigDecimal.valueOf(102)), None, None),
        StbRow(Some("a"), None, None, Some(math.BigDecimal.valueOf(103)), None, None),
        StbRow(Some("b"), None, None, Some(math.BigDecimal.valueOf(100)), None, None),
        StbRow(Some("b"), None, None, Some(math.BigDecimal.valueOf(101)), None, None))
      val order = Order(List("stb", "rev"))
      val result = order.apply(rows.iterator).toSeq
      for ((r, e) <- result.zip(expectedRows)) {
        println("result: ${r}")
        println("expected: ${e}")
        assert(r == e)
      }
    }

    {
      val expectedRows = Seq(StbRow(Some("b"), None, None, Some(math.BigDecimal.valueOf(100)), None, None),
        StbRow(Some("b"), None, None, Some(math.BigDecimal.valueOf(101)), None, None),
        StbRow(Some("a"), None, None, Some(math.BigDecimal.valueOf(102)), None, None),
        StbRow(Some("a"), None, None, Some(math.BigDecimal.valueOf(103)), None, None))
      val order = Order(List("rev", "stb"))
      val result = order.apply(rows.iterator).toSeq
      assert(result == expectedRows)
    }
  }
}
