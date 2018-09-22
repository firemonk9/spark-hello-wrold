package org.demo_etl

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Hello World")
      .enableHiveSupport()
      .getOrCreate()

    val jdbcDriverClass = "org.postgresql.Driver"
    val jdbcDriverPath = "/usr/hdp/2.6.5.0-292/oozie/libserver/postgresql-9.0-801.jdbc4.jar"
    val jdbcPassword = "postgres"
    val jdbcUrl = "jdbc:postgresql://some-postgres:5432/my_test"
    val jdbcUser = "postgres"

    val df = spark.read.format("JDBC").option("url", jdbcUrl).option("driver", jdbcDriverClass).option("dbtable", "projects").option("user", jdbcUser).option("password", jdbcPassword).load()

    println("read data from postgres ")
    df.show()

    df.write.mode(SaveMode.Overwrite).insertInto("mytable")

    spark.stop()
  }
}
