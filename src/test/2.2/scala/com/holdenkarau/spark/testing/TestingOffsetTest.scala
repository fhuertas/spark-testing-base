package com.holdenkarau.spark.testing

import org.scalacheck.Gen
import org.scalatest.{FunSuite, Matchers}

class TestingOffsetTest extends FunSuite with Matchers{

  test("Offset should serialized to json correctly") {
    val offsetNum = Gen.choose(10000,99999).sample.get
    val offset = TestingOffset(offsetNum)
    offset.json shouldBe s"""{"position":$offsetNum}"""
  }

  test("read offset from a json string") {
    val offsetNum = Gen.choose(10000,99999).sample.get
    val jsonString = s"""{"position":$offsetNum}"""

    val result = TestingOffset(jsonString)

    result shouldBe TestingOffset(offsetNum)
  }

}
