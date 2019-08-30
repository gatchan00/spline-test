package com.everis.gatchan.lineage

import java.util.Calendar
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import cats.implicits._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast

object LineageUtils {
  implicit class DataFrameLineage(df: DataFrame){
    def addMetaData(implicit spark:SparkSession) = {
      addLineageMetaDataInternal(df)
    }
    def kyberMap(implicit spark:SparkSession) = {

    }
  }


  def addLineageMetaDataInternal(input: DataFrame)(implicit spark:SparkSession) = {
    import spark.implicits._

    val generateUUID: String => String = _ + "::" + java.util.UUID.randomUUID.toString
    val generateUniqueIdUDF = udf(generateUUID)

    val timeStampAsString = Calendar.getInstance().getTimeInMillis.toString
    val lecturaTransformada = input
      .withColumn("lineageSourceId",lit("algo.csv"))
      .withColumn("lineageTimestampId",lit(timeStampAsString))
      .withColumn("lineageRowId",generateUniqueIdUDF('lineageSourceId))

    lecturaTransformada
  }
}
