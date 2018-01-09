package com.holdenkarau.spark.testing

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.DataSourceRegister
// TODO import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.{StringType, StructField, StructType}

private[testing] class TestingSourceProvider
  extends StreamSourceProvider with DataSourceRegister {
  // TODO extends of DataSourceRegister
  override def shortName: String = "testing-source"

  import TestingSourceProvider._

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) =
    (shortName, schema.getOrElse(DefaultSchema))

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): TestingSource = {
    require(parameters.contains(IdNs),
      s"TestingSourceProvider require that the property $IdNs is defined")
    val source = new TestingSource(schema.getOrElse(DefaultSchema),
      parameters(IdNs), sqlContext)
    sourcesMap = sourcesMap + (parameters(IdNs) -> source)
    source
  }

}

object TestingSourceProvider {
  val DefaultSchema = StructType(Seq(StructField("content", StringType)))

  val IdNs = "com.holdenkarau.spark.testing.source.id"

  var sourcesMap: Map[String, TestingSource] = Map.empty[String, TestingSource]

  def getById(id: String): Option[TestingSource] = sourcesMap.get(id)

  def unregisterSource(id: String): Option[TestingSource] = {
    val source = sourcesMap.get(id)
    sourcesMap = sourcesMap - id
    source
  }
}
