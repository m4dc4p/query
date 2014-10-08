package com.codeslower.query

import java.io.{BufferedReader, FileReader}
import java.nio.charset.Charset
import java.nio.file.{Paths, Files}
import java.text.DecimalFormat
import java.util.concurrent.TimeUnit

import org.apache.commons.csv.{CSVRecord, CSVFormat}

import scala.concurrent.duration.Duration
import scala.util.Try
import scala.collection.JavaConverters._

object Boot extends App {
  /**
   * Assume filter operations are passed as comma-separated arguments.
   * Assume ordering is always descending
   * Assume empty string/missing value is NULL (not empty string)
   * Assume currency values DO NOT include currency symbol; assuming no negative numbesr.
   * Assuming currencies under $1.00 have the form "0.xx"; assuming all currencies specify
   * cents (0.90, not 0.9).
   */

  /*
  Reader in = new StringReader("a,b,c");
  for (CSVRecord record : CSVFormat.DEFAULT.parse(in)) {
    for (String field : record) {
      System.out.print("\"" + field + "\", ");
    }
    System.out.println();
  }
  */

  val revenueParser = new DecimalFormat("#,###.##")
  revenueParser.setParseBigDecimal(true)
  val dateParser = new java.text.SimpleDateFormat("yyyy-MM-dd")

  val file = Files.newBufferedReader(Paths.get("./rows.txt"), Charset.forName("UTF-8"))
  val rows : Iterable[StbRow] = for(r : CSVRecord <- CSVFormat.DEFAULT.withDelimiter('|').withHeader().parse(file).asScala)
    yield new StbRow(Try {
      r.get("STB")
    }.toOption, Try {
      dateParser.parse(r.get("DATE"))
    }.toOption, Try {
      val v = r.get("VIEW_TIME").split(":")
      Duration(Integer.parseInt(v(0)) * 60 + Integer.parseInt(v(1)), TimeUnit.MINUTES)
    }.toOption, Try {
      revenueParser.parse(r.get("REV")).asInstanceOf[java.math.BigDecimal]
    }.toOption, Try {
      r.get("TITLE")
    }.toOption, Try {
      r.get("PROVIDER")
    }.toOption)

  val filterExprs : List[(String, String)] = List()
  val selectExprs : List[String] = List()
  val orderExprs : List[String] = List()

  val sorter : Order[StbRow] = Order(List())
  val filter : Filter[StbRow] = Filter(List())
  val select : Select[Row] = Select(List())

  val processor = select compose sorter compose filter

  for(r <- processor(rows.iterator)) {
    val row = for (c <- r.columns) yield r.column(c)
    println(row.map(c => c.getOrElse("")).mkString(","))
  }
}
