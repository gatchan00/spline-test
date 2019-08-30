package com.everis.gatchan

import java.io.File

import org.apache.spark.sql.{DataFrame, Row, RowFactory, SaveMode, SparkSession}
import cats.implicits._
import com.everis.gatchan.configs.Config.ExampleConfig
import com.everis.gatchan.utils.GatchanSparkWrapper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import pureconfig.generic.ProductHint
import pureconfig.{CamelCase, ConfigFieldMapping}
import pureconfig.generic.auto._

import com.everis.gatchan.configs.Config._
import com.everis.gatchan.lineage.LineageUtils._


/**
  * Basic skeleton for launching spark applications
  * It is mandatory to receive the config file as parameter
  * also, you need to create a case class with the configuration, as shown in config.Configs
  * This example loads the configuration from that file, either if its on local filesystem or if you give a dfs path
  * Example paths may be:
  *   D:\\Everis\\Workspaces\\santander\\BasicExample\\src\\main\\resources\\configExample.cfg
  *   file:///home/file.conf
  *   hdfs://Miskatonic.Arkham:9000/configs/join.config
  *
  *   Yo need to uncomment the .master("local") to run locally. Remember also to change the appName
  * */
object BasicExampleKafka  extends App {
  //No quitar, es necesario
  implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  //1- Definimos cómo se crea la sesión de spark
  def maybeSpark: SparkSession = {
    SparkSession.builder()
      .appName("DataLineageWrapperKafka")
      .master("local") //uncoment to run locally
      .getOrCreate()
  }

  //2- defino mi job
  def job(conf: ExampleConfig, spark: SparkSession) = {

    /** Pon aquí tu código**/

    import za.co.absa.spline.core.SparkLineageInitializer._
    spark.enableLineageTracking()

    import spark.implicits._
    val lectura1: DataFrame = spark
      .read.
      format("csv")
      .option("header", "true")
      .load("algo.csv")

    val lectura2: DataFrame = spark
      .read.
      format("csv")
      .option("header", "true")
      .load("compras.csv")

    //val lecturaMetadatada = lectura.addMetaData(spark)


    val mezcla = lectura1.join(lectura2, lectura1("id") === lectura2("customerid"))

    val mezcla2 = mezcla.withColumn("Nueva",'age  * 3)
    mezcla2.selectExpr("CAST (Nueva as STRING) as value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "topic_spline")
      .save()

  }

  //3- Llamo al wrapper
  val result = GatchanSparkWrapper.runSparkProcess(args, maybeSpark, job)

  //4- controlamos errores
  result match{
    case Left(e) => {e.printStackTrace()}
    case _ => println("Finalizado con éxito")
  }
}
