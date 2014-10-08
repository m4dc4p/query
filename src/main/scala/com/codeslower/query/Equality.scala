package com.codeslower.query

class Equality[A <: StbRow] extends Predicate[A] {
  override def test(row: A): Boolean = false
}
