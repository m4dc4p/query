package com.codeslower.query

import java.io.{InputStream, FileInputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.nio.channels.{FileChannel, SeekableByteChannel}
import java.nio.file.{StandardOpenOption, Path}
import scala.util.{Failure, Success, Try}

trait FileStore[R <: Row] extends Iterable[R] {
  def schema: Schema
  def getAt(idx: Int): Try[Option[Array[Byte]]]
  def hasAt(idx: Int): Try[Option[Unit]]
}

class StbIndexedFile(path: Path) extends FileStore[StbIndexedRow] {

  private val file: FileChannel = FileChannel.open(path, StandardOpenOption.READ)
  file.map(MapMode.READ_ONLY, 0, file.size())

  def getAt(idx: Int): Try[Option[Array[Byte]]] = Try {
    val buffer: ByteBuffer = ByteBuffer.allocate(schema.recordSize)
    val bytesRead = file.read(buffer, idx * (schema.recordSize + 1)) // one extra byte for newline
    if(bytesRead != schema.recordSize) {
      throw new RuntimeException("No record found.") // EOF or past range
    }
    else if (buffer.get(0) == 0) {
      None // Empty record, skip
    }
    else {
      val bytes: Array[Byte] = new Array(buffer.capacity())
      buffer.rewind()
      buffer.get(bytes)
      // print(Math.floor(Math.log10(idx)))
      Some(bytes)
    }
  }

  def hasAt(idx: Int): Try[Option[Unit]] = Try {
    val buffer: ByteBuffer = ByteBuffer.allocate(1)
    val bytesRead = file.read(buffer, idx * (schema.recordSize + 1)) // one extra byte for newline
    if(bytesRead != 1) {
      throw new RuntimeException("No record found.") // EOF or past range
    }
    else if (buffer.get(0) == 0) {
      None // Empty record, skip
    }
    else {
      Some(Unit)
    }
  }

  override def iterator: Iterator[StbIndexedRow] = {
    val store = this
    new Iterator[StbIndexedRow] {
      var finished = false
      var i = 0

      private def read(startAt: Int): Option[Int] = {
        hasAt(startAt) match  {
          case Success(Some(r)) => Some(startAt)
          case Success(None) => read(startAt + 1)
          case Failure(_) => None
        }
      }

      override def hasNext: Boolean = read(i).isDefined

      override def next(): StbIndexedRow = read(i) match {
        case Some(idx) => {
          i = idx + 1
          new StbIndexedRow(idx, store)
        }
        case _ => throw new RuntimeException("Iterator exhausted.")
      }
    }
  }

  override def schema: StbSchema = StbSchema.instance

}
