package com.codeslower.importer

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import java.nio.file._
import java.util.Objects
import com.codeslower.query.{StbSchema}
import org.apache.commons.csv.{CSVFormat, CSVRecord}
import scala.collection.mutable
import scala.collection.JavaConverters._

case class InputRecord(val stb: Option[String],
                       val date: Option[String],
                       val viewTime: Option[String],
                       val revenue: Option[String],
                       val title: Option[String],
                       val provider: Option[String])

object Boot extends App {

  if(this.args.length !=1 ) {
    println("The filename to import must be provided as an argument.")
    System.exit(1)
  }

  val inputFilePath: Path = Paths.get(s"./${this.args(0)}")
  val tmpFilePath : Path = Paths.get("./rows.tmp")
  val fileStorePath : Path = Paths.get("./rows.dat")

  private val inputFile = Files.newBufferedReader(inputFilePath, Charset.forName("UTF-8"))
  private val tmpFile = FileChannel.open(tmpFilePath,
    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE)

  private val rows : Iterable[InputRecord] = for(r : CSVRecord <- CSVFormat.DEFAULT.withDelimiter('|').withHeader().parse(inputFile).asScala)
    yield new InputRecord(Option(r.get("STB")),
      Option(r.get("DATE")),
      Option(r.get("VIEW_TIME")),
      Option(r.get("REV")),
      Option(r.get("TITLE")),
      Option(r.get("PROVIDER")))

  println("Importing data ...")
  val records: mutable.Map[Long, Long] = new mutable.LongMap()
  val dups: mutable.Map[Long, List[Long]] = new mutable.LongMap()

  var count = 0
  for((r, idx) <- rows.zip(Stream.from(0))) {
    val rowArr: Array[Byte] = new Array(StbSchema.instance.recordSize + 1)
    for(stb <- r.stb; date <- r.date; viewTime <- r.viewTime; revenue <- r.revenue; title <- r.title; provider <- r.provider) {
      val key = Objects.hash(stb.trim, title.trim, date.trim).toLong
      for(lastIdx <- records.get(key)) {
        dups.get(key) match {
          case Some(idxs) => dups.put(key, idx :: idxs)
          case _ => dups.put(key, List(idx, lastIdx))
        }
      }
      records.put(key, idx)

      System.arraycopy(stb.padTo(StbSchema.instance.stblen, " ").mkString.getBytes("UTF-8"),
        0, rowArr,
        StbSchema.instance.stbpos, StbSchema.instance.stblen)

      System.arraycopy(title.padTo(StbSchema.instance.titlelen, " ").mkString.getBytes("UTF-8"),
        0, rowArr,
        StbSchema.instance.titlepos, StbSchema.instance.titlelen)

      System.arraycopy(provider.padTo(StbSchema.instance.providerlen, " ").mkString.getBytes("UTF-8"),
        0, rowArr,
        StbSchema.instance.providerpos, StbSchema.instance.providerlen)

      System.arraycopy(date.padTo(StbSchema.instance.datelen, " ").mkString.getBytes("UTF-8"),
        0, rowArr,
      StbSchema.instance.datepos, StbSchema.instance.datelen)

      System.arraycopy(revenue.padTo(StbSchema.instance.revlen, " ").mkString.getBytes("UTF-8"),
        0, rowArr,
        StbSchema.instance.revpos, StbSchema.instance.revlen)

      System.arraycopy(viewTime.padTo(StbSchema.instance.viewlen, " ").mkString.getBytes("UTF-8"),
        0, rowArr,
        StbSchema.instance.viewpos, StbSchema.instance.viewlen)

      rowArr(StbSchema.instance.recordSize) = '\n'.toByte

      tmpFile.write(ByteBuffer.wrap(rowArr))
    }

    count = idx
  }

  records.clear()

  println(s"Imported ${count + 1} rows.")

  private val dupIdxs: Iterable[Long] = dups.values.map(_.drop(1).reverse).flatten
  println(s"Cleaning up ${dupIdxs.size} duplicates...")

  val emptyRow: Array[Byte] = new Array[Byte](StbSchema.instance.recordSize + 1)
  emptyRow(StbSchema.instance.recordSize) = '\n'.toByte

  for(idx <- dupIdxs) {
    tmpFile.write(ByteBuffer.wrap(emptyRow), idx * (StbSchema.instance.recordSize + 1))
  }

  inputFile.close()
  tmpFile.close()

  println("Copying temp file to datastore ...")
  Files.move(tmpFilePath, fileStorePath, StandardCopyOption.REPLACE_EXISTING)
}
