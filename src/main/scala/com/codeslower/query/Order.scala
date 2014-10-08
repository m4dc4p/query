package com.codeslower.query

trait Order[A <: Row] extends Function1[Iterator[A], Iterator[A]] {
  def apply(rows : Iterator[A]) : Iterator[A]
}

object Order {
  def makeComparator(col: String): Ordering[StbRow] = col.toUpperCase.trim   match {
    case "STB" => new Ordering[StbRow] {
      override def compare(x: StbRow, y: StbRow): Int =
        (x.stb, y.stb) match {
          case (Some(r1), Some(r2)) => r1.compareTo(r2)
          case (Some(r1), _) => 1
          case (_, Some(r2)) => -1
          case (_, _) => 0
        }
    }
    case "TITLE" => new Ordering[StbRow] {
      override def compare(x: StbRow, y: StbRow): Int =
        (x.title, y.title) match {
          case (Some(r1), Some(r2)) => r1.compareTo(r2)
          case (Some(r1), _) => 1
          case (_, Some(r2)) => -1
          case (_, _) => 0
        }
    }
    case "PROVIDER" => new Ordering[StbRow] {
      override def compare(x: StbRow, y: StbRow): Int =
        (x.provider, y.provider) match {
          case (Some(r1), Some(r2)) => r1.compareTo(r2)
          case (Some(r1), _) => 1
          case (_, Some(r2)) => -1
          case (_, _) => 0
        }
    }
    case "DATE" => new Ordering[StbRow] {
      override def compare(x: StbRow, y: StbRow): Int =
        (x.provider, y.provider) match {
          case (Some(r1), Some(r2)) =>  r1.compareTo(r2)
          case (Some(r1), _) => 1
          case (_, Some(r2)) => -1
          case (_, _) => 0
        }
    }
    case "REV" => new Ordering[StbRow] {
      override def compare(x: StbRow, y: StbRow): Int =
        (x.revenue, y.revenue) match {
          case (Some(r1), Some(r2)) => r1.compareTo(r2)
          case (Some(r1), _) => 1
          case (_, Some(r2)) => -1
          case (_, _) => 0
        }
    }
    case "VIEW_TIME" => new Ordering[StbRow] {
      override def compare(x: StbRow, y: StbRow): Int =
        (x.viewTime, y.viewTime) match {
          case (Some(r1), Some(r2)) => r1.compareTo(r2)
          case (Some(r1), _) => 1
          case (_, Some(r2)) => -1
          case (_, _) => 0
        }
    }
  }

  def makeOrder(cols: List[String], ordering: Ordering[StbRow]): Ordering[StbRow] = {
    cols match {
      case Nil => new Ordering[StbRow] {
        override def compare(r1: StbRow, r2: StbRow): Int = ordering.compare(r1, r2)
      }
      case col :: rest => {
        val lastOrdering = makeComparator(col)
        makeOrder(rest,  new Ordering[StbRow] {
          override def compare(r1: StbRow, r2: StbRow): Int = { ordering.compare(r1, r2) match {
            case 0 => lastOrdering.compare(r1, r2)
            case x => x
          }}
        })
      }
    }
  }

  def apply(columns : List[String]): Order[StbRow] = {
    columns match {
      // No need to sort in this case
      case Nil => new Order[StbRow] {
        override def apply(rows: Iterator[StbRow]): Iterator[StbRow] = rows
      }
      case _ => {
        val orderer = makeOrder(columns, new Ordering[StbRow] {
          override def compare(r1: StbRow, r2: StbRow): Int = 0
        })

        // Unfortunately this sorts in-memory
        new Order[StbRow] {
          override def apply(rows: Iterator[StbRow]): Iterator[StbRow] = {
            rows.toList.sorted(orderer).iterator
          }
        }
      }
    }
  }
}
