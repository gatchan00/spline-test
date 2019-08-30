package com.everis.gatchan.utils

import java.io.File
import java.util.UUID.randomUUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import pureconfig.{CamelCase, ConfigFieldMapping}
import pureconfig.generic.ProductHint
import cats.implicits._


object GatchanSparkWrapper {
    /**
      * Function that retrieves a file from a local or remote directory (hdfs, wasb, adlss...) and copies it to the local file system, giving a UUID name.
      * @param spark a SparkSession to retrieve the hadoop configuration
      * @param origin The location of the file that must be retrieved
      * @return The name of the file (the UUID generated)
      * */
    def copyRemoteFileToLocal(spark: SparkSession, origin: String) = {
      var fs: FileSystem = null
      if (origin.indexOf("//") == -1){fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)}
      else{
        val beforeServer = origin.indexOf("//") + "//".length
        val afterServer = origin.indexOf("/",beforeServer)
        val serverPath =  origin.substring(0,afterServer)

        if (beforeServer!=afterServer) {
          val conf:Configuration = new Configuration()
          conf.set("fs.defaultFS",serverPath)
          fs = FileSystem.get(conf)
        }
        else {
          fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        }
      }
      val hdfsPath: Path = new Path(origin)
      val destination = randomUUID().toString
      val localPath: Path = new Path(destination)
      fs.copyToLocalFile(false, hdfsPath, localPath, true)
      destination
    }

    /**
      * Function that copies a file from a local  directory to the local remote dfs.
      * @param spark a SparkSession to retrieve the hadoop configuration
      * @param origin The location of the file that must be copy to the dfs
      * @param destination The location to copy the file
      * @return The name of the file (the UUID generated)
      * */
    def copyLocalFileToRemote(spark: SparkSession, origin: String, destination: String) = {
      var fs: FileSystem = null
      if(destination.indexOf("//") == -1) {
        fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      }
      else {
        val beforeServer = destination.indexOf("//") + "//".length
        val afterServer = destination.indexOf("/", beforeServer)
        val serverPath = destination.substring(0, afterServer)

        if(beforeServer != afterServer) {
          val conf: Configuration = new Configuration( )
          conf.set("fs.defaultFS", serverPath)
          fs = FileSystem.get(conf)
        }
        else {
          fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        }
      }
      val hdfsPath: Path = new Path(destination)
      val localPath: Path = new Path(origin)

      Either.catchNonFatal(fs.copyFromLocalFile(localPath, hdfsPath))
    }

    /**
      * Function runs a job on spark cluster loading the configuration
      * @param args arguments retrieved from the command line. Must hold the path of the config file
      * @param maybeSpark SparkSession needed to run the job
      * @param jobFunction The jub that will be launched
      * @return an Either with an error or a Unit type
      * */
    def runSparkProcess[ConfigClass, T](args: Array[String],
                                        maybeSpark: SparkSession,
                                        jobFunction: (ConfigClass, SparkSession) => Unit )
                                       (implicit reader: pureconfig.Derivation[pureconfig.ConfigReader[ConfigClass]],
                                        hint: ProductHint[T]): Either[Throwable, Unit.type] =
    {
      implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
      val maybeConfig: Either[Throwable, (ConfigClass, SparkSession)] = {
        for {
          spark <- Either.catchNonFatal(maybeSpark)
          configPath <- args.headOption.filter(!_.isEmpty).toRight(new NoSuchElementException("Empty arguments"))
          destination <- Either.catchNonFatal(copyRemoteFileToLocal(spark, configPath))
          config <- pureconfig.loadConfig[ConfigClass](new File(destination).toPath).leftMap(e => new Exception(e.toList.mkString(", ")))
          _ <- Either.catchNonFatal{(new File(destination)).delete()}
        } yield {
          (config, spark)
        }
      }

      maybeConfig match{
        case Left(e) => {
          Left(e)
        }
        case Right(configandSession) =>
        {
          val (appConfig, spark) = configandSession
          import spark.implicits._
          jobFunction(appConfig,spark)

          spark.close()
          Right(Unit)
        }
      }
      //println("fin")
    }

    /**
      * method that writes to Hive
      * @param ds Dataseet to be written to hive
      * @param saveMode savemode default value SaveMode.Append
      * @param tableName the table name in Hive
      * @tparam T the type of the dataset ds
      */
    def writerHive[T](ds: Dataset[T], saveMode: SaveMode = SaveMode.Append, tableName:String): Unit = {
      ds.write
        .format("orc")
        .mode(saveMode)
        .saveAsTable(tableName)
    }
  }
