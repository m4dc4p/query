package com.codeslower.query

import java.nio.file.{Paths, Files}

object Boot extends App {
  /**
   * Assume filter operations are passed as comma-separated arguments.
   * Assume ordering is always descending
   * Assume empty string/missing value is NULL (not empty string)
   * Assume currency values DO NOT include currency symbol; assuming no negative numbesr.
   * Assuming currencies under $1.00 have the form "0.xx"; assuming all currencies specify
   * cents (0.90, not 0.9).
   *
   * Assuming no bad data (NULL values)
   */

  val indexedFile: FileStore[StbIndexedRow] = new StbIndexedFile(Paths.get("/tmp/rows.dat"))

  val filterExprs : List[(String, String)] = List(("provider", "buena vista"), ("rev", "6.00"))
  val selectExprs : List[String] = List()
  val orderExprs : List[String] = List("STB")

  val sorter: Order[StbIndexedRow] = Order(orderExprs)
  val filter: Filter[StbIndexedRow] = Filter(filterExprs)
  val select : Select[Row] = Select(selectExprs)

  val processor: Iterator[StbIndexedRow] => Iterator[Row] = select compose sorter compose filter

  for(r <- processor(indexedFile.iterator)) {
    val row = for (c <- r.schema.columns) yield {
      r.column(c)
    }
    println(row.map(c => c.getOrElse("")).mkString(","))
  }
}
