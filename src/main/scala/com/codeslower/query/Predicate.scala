package com.codeslower.query

trait Predicate[A <: StbRow] {
  def test(row : A) : Boolean
}
