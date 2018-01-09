package com.holdenkarau.spark.testing

import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types._
import org.scalacheck.Gen
import org.scalatest.{FunSuite, Matchers}

class TestingSourceProviderIT extends FunSuite with Matchers
  with SharedSparkContext with StructuredStreamingBase {

}

class TestingSourceProviderTest
  extends FunSuite with Matchers {

  val provider = new TestingSourceProvider()

  test("get the schema that is indicated") {
    val testSchema: StructType = StructType(Seq(
      StructField("filed1", BinaryType),
      StructField("filed2", LongType),
      StructField("filed3", StringType),
      StructField("filed4", IntegerType)))

    val (shortName, schema) = provider.sourceSchema(
      null, Some(testSchema), "provider", Map.empty[String, String])

    shortName shouldBe "testingSource"
    schema shouldBe testSchema
  }

  test("get a default schema it is not defined") {
    val (shortName, schema) = provider.sourceSchema(
      null, None, "provider", Map.empty[String, String])

    shortName shouldBe "testingSource"
    // Default schema
    schema shouldBe StructType(Seq(StructField("content", StringType)))
  }

  test("the source cannot be created without id") {
    an[IllegalArgumentException] shouldBe thrownBy(
      provider.createSource(
        null, "", None, "provider", Map.empty[String, String]))


  }

  test("create a source with id and it is set") {
    val id = Gen.choose(0, 1000).sample.get.toString
    val result = provider.createSource(
      null, "", None, "provider",
      Map("com.holdenkarau.spark.testing.source.id" -> id))
    result.id shouldBe id
  }

  test("when try to recover a datasource that doesn't exist. The result is None") {
    val id = Gen.choose(0, 1000).sample.get.toString
    val result = TestingSourceProvider.getById(id)
    result shouldBe None
  }

  test("a datasource create can be recovered with id") {
    val provider = new TestingSourceProvider()
    val id = Gen.choose(0, 1000).sample.get.toString
    provider.createSource(
      null, "", None, "provider",
      Map("com.holdenkarau.spark.testing.source.id" -> id))


    val result = TestingSourceProvider.getById(id)
    result.map(_.id) shouldBe Some(id)
  }
}

class TestingSourceProviderIT
  extends FunSuite
    with Matchers
    with SharedSparkContext
    with StructuredStreamingBase {

  test("a source should be unregister correcly") {
    val id = Gen.choose(100000, 999999).sample.get.toString
    val source = new TestingSource(
      TestingSourceProvider.DefaultSchema, id, spark.sqlContext)

    TestingSourceProvider.sourcesMap = Map(id -> source)

    val result = TestingSourceProvider.unregisterSource(id)

    TestingSourceProvider.sourcesMap.contains(id) shouldBe false

    result shouldBe Some(source)

  }

  test("when try to remove a source that not exists the return should be None") {
    val id = Gen.choose(100000, 999999).sample.get.toString

    val result = TestingSourceProvider.unregisterSource(id)

    result shouldBe None
  }

}
