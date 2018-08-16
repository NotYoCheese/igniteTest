package com.spec.app

import org.apache.ignite.cache.query.annotations.QuerySqlField
import scopt.OptionParser
import org.apache.ignite.spark.{IgniteContext, IgniteDataFrameSettings}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.annotation.meta.field

/**
  *
  * Created by: mike 
  * Date: 8/14/18
  * Time: 11:19 AM
  * To change this template use Preferences/Editor/File and Code Templates/Includes
  */

case class Person( @(QuerySqlField @field)(index = true)user_id:Long,  @(QuerySqlField @field)name:String)
case class Config(mode:String = "doAll")

object IgniteTest {

  val configFile = "personCacheDev.xml"

  var appConfig:Config = _
  var spark:SparkSession = _
  def createSparkSesion():SparkSession = {
    SparkSession.builder
      .appName("IgniteTest").getOrCreate()
  }

  def createCache(config:Config, spark:SparkSession):Unit = {
    System.out.println("Saving Dataset as Ignite Cache")
    import spark.implicits._
    val personDS:Dataset[Person] = Seq(Person(1, "Jim"), Person(2, "Ann"),
      Person(3, "Mike"),Person(4, "Lynn"), Person(5, "John")).toDS()
    personDS.write.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .mode(SaveMode.Overwrite)
      .option(IgniteDataFrameSettings.OPTION_TABLE, "PERSON")
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, configFile)
      .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "user_id").save()
    System.out.println("Done - Saving Dataset as Ignite Cache")

  }

  def readAsDataset(config:Config, spark:SparkSession):Unit = {
    import spark.implicits._

    System.out.println("Reading Dataset from Ignite Cache")
    val personDS:Dataset[Person] = spark.read.format(IgniteDataFrameSettings.FORMAT_IGNITE)
      .option(IgniteDataFrameSettings.OPTION_TABLE,  "SQL_PUBLIC_TAPADAAID")
      .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, configFile)
      .load().as[Person]
    System.out.println(s"Dataset contains ${personDS.count()} records:")
//    personDS.map( person =>
//      System.out.println(s"id: ${person.id}\tName: ${person.name}")
//    )

    System.out.println("Done - Reading Dataset from Ignite Cache")
  }

  def readAsRDD(config:Config, spark:SparkSession):Unit = {

    System.out.println("Reading RDD from Ignite Cache")
    val igniteContext = new IgniteContext(spark.sparkContext, configFile)
    val cache = igniteContext.fromCache("SQL_PUBLIC_PERSON")

    System.out.println(s"rdd contains ${cache.count()} records:")
    cache map { person =>
      System.out.println(s"id: ${person._1}\tName: ${person._2}")
    }
    System.out.println("Done - Reading RDD from Ignite Cache")
  }

  def doAll(config:Config, spark:SparkSession):Unit = {
    createCache(config, spark)
    readAsDataset(config, spark)
    readAsRDD(config, spark)
  }

  def main(args:Array[String]):Unit = {
    parser.parse(args, Config()) match {
      case Some(config) =>
        appConfig = config
        spark = createSparkSesion()
        config.mode match {
          case "createCache" => createCache(config, spark)
          case "readAsDataset" => readAsDataset(config, spark)
          case "readAsRDD" => readAsRDD(config, spark)
          case "doAll" => doAll(config, spark)
        }
        spark.stop()
      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

  val parser:OptionParser[Config] = new scopt.OptionParser[Config]("Ignite Test")  {
    head("Ignite Test", "0.0.1")
    cmd("createCache").action((_, c) => c.copy(mode = "createCache"))
      .text("create the cache and exit")
    cmd("readAsDataset").action((_, c) => c.copy(mode = "createCache"))
      .text("read the cache as a Dataset and exit")
    cmd("readAsRDD").action((_, c) => c.copy(mode = "createCache"))
      .text("read the cache as a RDD and exit")
    cmd("doAll").action((_, c) => c.copy(mode = "createCache"))
      .text("create the cache and read both ways")
  }
}
