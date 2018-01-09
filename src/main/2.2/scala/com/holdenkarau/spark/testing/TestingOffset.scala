package com.holdenkarau.spark.testing

import org.apache.spark.sql.execution.streaming.Offset
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

case class TestingOffset(position: Int) extends Offset {
  override def json: String = TestingOffset.toJsonString(this)
}

object TestingOffset {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def apply(json: String): TestingOffset = read[TestingOffset](json)

  def toJsonString(offset: TestingOffset): String = write(offset)
}
