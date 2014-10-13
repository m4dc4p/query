package com.codeslower.query

import java.io.{PrintWriter, OutputStreamWriter, StringWriter}
import java.nio.file.{Paths, Files}

import org.apache.commons.cli._

import scala.util.{Try, Success, Failure}

object Boot extends App {
  val opts = new Options
  opts.addOption("s",true, "A comma-separated list of columns to display. Can only be used once.")
  opts.addOption("o",true, "A comma-separated list of columns to order results by. Can only be used once.")
  opts.addOption("f",true, "A filter expressions, with the form 'COLUMN=VALUE'. This argument may appear any number of times.")
  opts.addOption("h", "help", false, "Display this help.")

  private def printUsage(): Nothing = {
    new HelpFormatter().printUsage(new PrintWriter(System.out), 80, "query", opts)
    System.exit(0)
    ???
  }

  private val queryArgs  = Try { new PosixParser().parse(opts, this.args) } match {
    case Success(a) => a
    case Failure(_:ParseException) => printUsage()
    case Failure(t) => throw t
  }

  val indexedFile: FileStore[StbIndexedRow] = new StbIndexedFile(Paths.get("./rows.dat"))

  if(queryArgs.hasOption('h')) {
    printUsage()
  }

  val filterExprs : List[(String, String)] = queryArgs.getOptionValues('f') match {
    case filters if filters != null && filters.length > 0 =>
      filters.map((expr:String) =>
          expr.split("=").map(_.trim))
        .map(f =>
          (f(0), f(1))).toList
    case _ => List()
  }
  val selectExprs : List[String] = queryArgs.getOptionValue('s') match {
    case exprs if exprs != null =>
      exprs.split(',').toList.map(_.trim)
    case _ => List()
  }
  val orderExprs : List[String] = queryArgs.getOptionValue('o') match {
    case exprs if exprs != null =>
      exprs.split(',').toList.map(_.trim)
    case _ => List()
  }

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
