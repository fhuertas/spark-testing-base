package com.holdenkarau.spark.testing

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Row, SQLContext}

private[testing] class TestingSource(val schema: StructType,
                                     val id: String,
                                     val sqlContext: SQLContext)

  extends Source with Logging {

  import TestingSource._
  import org.apache.spark.sql.functions._

  var memory: DataFrame = initMemory

  def initMemory: DataFrame = sqlContext.createDataFrame(
    sqlContext.sparkContext.emptyRDD[Row],
    schema.add(PosColumnName, LongType, nullable = false))

  var offset: Option[TestingOffset] = None

  override def getOffset: Option[Offset] = {
    // TODO revise logs levels
    logDebug("Reading Offset")
    offset
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logDebug(s"Start from ${start.getOrElse("Beginning")} to $end")
    val initDF = start.map(
      offset => TestingOffset(offset.json).position).getOrElse(0)
    val endDF = TestingOffset(end.json).position
    memory.filter(col(PosColumnName) >= initDF and col(PosColumnName) < endDF)
      .drop(PosColumnName)
  }

  override def stop(): Unit = {
    memory = initMemory
  }

  def addBatch[A: Encoder](data: Seq[A]): Unit = {
    import sqlContext.implicits._
    val initPos = offset.map(_.position).getOrElse(0)

    val newMemory = addIndex(data.toDF, initPos)(sqlContext)

    memory = memory.union(newMemory)
    offset = Some(TestingOffset(
      offset.map(_.position).getOrElse(0) +  data.length))
  }
}

object TestingSource {
  val PosColumnName = "__pos__"

  def addIndex(df: DataFrame, start: Int)
              (implicit sqlContext: SQLContext): DataFrame = {
    val newData = df.rdd.zipWithIndex() map {
      case (row, index) => Row.fromSeq(row.toSeq :+ (index + start))
    }
    val newSchema = df.schema.add(PosColumnName, LongType, nullable = false)

    sqlContext.createDataFrame(newData, newSchema)
  }
}