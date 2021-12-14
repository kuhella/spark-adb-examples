package io.arenadata.spark.examples

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

object flow  extends App with SparkSessionWrapper {

  val adbUrl = args(0)
  val adbUser = args(1)
  val adbPass = args(2)
  val sourceAdbSchema = args(3)
  val sourceAdbTable= args(4)

  val targetAdbSchema= args(5)
  val targetAdbTable = args(6)


    val sourceDf = spark
      .read
      .format("adb")
      .option("spark.adb.url", adbUrl)
      .option("spark.adb.user", adbUser)
      .option("spark.adb.password", adbPass)
      .option("spark.adb.dbschema", sourceAdbSchema)
      .option("spark.adb.dbtable", sourceAdbTable)
      .load()

    sourceDf
      .write
      .format("adb")
      .option("spark.adb.url", adbUrl)
      .option("spark.adb.user", adbUser)
      .option("spark.adb.password", adbPass)
      .option("spark.adb.dbschema", targetAdbSchema)
      .option("spark.adb.dbtable", targetAdbTable)
      .mode(SaveMode.Overwrite)
      .save()
}

