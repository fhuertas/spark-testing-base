package com.holdenkarau.spark.testing

import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.scalacheck.Gen
import org.scalatest.{FunSuite, Matchers}

class TestingSourceTest extends FunSuite
  with Matchers
  with SharedSparkContext
  with StructuredStreamingBase {

  val provider = new TestingSourceProvider()

  import spark.implicits._

  val schema = StructType(Seq(
    StructField("string", StringType),
    StructField("int", IntegerType)))

  def createSource(id: String, schema: StructType): DataStreamReader = {
    spark.readStream.format("com.holdenkarau.spark.testing.TestingSourceProvider")
      .option(TestingSourceProvider.IdNs, id).schema(schema)
  }

  def genData(size: Int): Gen[Seq[(String, Int)]] = Gen.listOfN(size, for {
    a <- Gen.choose(1000000, 9999999).map(_.toString)
    b <- Gen.choose(10000, 99999)
  } yield (a, b))


  test("set correctly the schema when create") {
    val testSchema: StructType = StructType(Seq(
      StructField("filed1", BinaryType),
      StructField("filed2", LongType),
      StructField("filed3", StringType),
      StructField("filed4", IntegerType)))

    val result = provider.createSource(
      spark.sqlContext, "", Some(testSchema), "",
      Map(TestingSourceProvider.IdNs -> "id"))

    result.schema shouldBe testSchema

  }

  test("set correctly default schema") {
    val result = provider.createSource(
      spark.sqlContext, "", None, "",
      Map(TestingSourceProvider.IdNs -> "id"))

    // Default schema
    result.schema shouldBe StructType(Seq(StructField("content", StringType)))

  }

  test("get offset should return none if there are not data") {
    val result = provider.createSource(
      spark.sqlContext, "", None, "",
      Map(TestingSourceProvider.IdNs -> "id"))

    result.getOffset shouldBe None

  }

  test("It is possible to add data to the Source and it's correctly added") {
    val tableName = s"t1${System.currentTimeMillis()}"
    val id = Gen.choose(1, 100000).sample.get.toString
    spark.sparkContext.setLogLevel("INFO")
    val size = Gen.choose(0, 10).sample.get
    val data = genData(size).sample.get

    val query = createSource(id, schema).load().writeStream
      .queryName(tableName).format("memory").start()

    Thread.sleep(1000)
    val source = TestingSourceProvider.getById(id).get
    source.addBatch(data)
    query.awaitTermination(2000)


    val offset = source.getOffset
    offset shouldBe Some(TestingOffset(size))
    val result = spark.sql(s"select * from $tableName").as[(String, Int)].collect()
    result.length shouldBe size
    result.toSet shouldBe data.toSet
  }

  test("The dataframe should be appended, not replaced") {
    val tableName = s"t2${System.currentTimeMillis()}"
    val id = Gen.choose(1, 100000).sample.get.toString
    spark.sparkContext.setLogLevel("INFO")
    val (s1, s2) = (Gen.choose(1, 10).sample.get, Gen.choose(1, 10).sample.get)
    val (d1, d2) = (genData(s1).sample.get, genData(s2).sample.get)

    val query = createSource(id, schema).load().writeStream
      .queryName(tableName).format("memory").start()

    Thread.sleep(500)
    val source = TestingSourceProvider.getById(id).get
    source.addBatch(d1)
    Thread.sleep(500)
    source.addBatch(d2)
    query.awaitTermination(2000)

    val result = spark.sql(s"select * from $tableName").as[(String, Int)].collect()
    result.length shouldBe s1 + s2
  }

  test("should add the index correcly") {
    val s1 = Gen.choose(1, 10).sample.get
    val d1 = genData(s1).sample.get

    val start = Gen.choose(0, 100).sample.get

    val expected: Seq[Row] = d1.indices map (
      i => Row(d1(i)._1, d1(i)._2, i + start))

    val result = TestingSource.addIndex(d1.toDF,start)

    result.toDF.collect().toSet shouldBe expected.toSet
  }

  test("a source when stop the dataframe should be empty") {
    val id = Gen.choose(1, 100000).sample.get.toString
    val size = Gen.choose(1, 10).sample.get
    val data = genData(size).sample.get

    val source = new TestingSource(
      TestingSourceProvider.DefaultSchema, id, spark.sqlContext)
    source.memory = data.toDF

    source.stop()

    source.memory.collect() shouldBe empty

  }
}
