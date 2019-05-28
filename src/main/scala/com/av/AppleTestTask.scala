package com.av

import org.apache.spark.sql.SparkSession

object AppleTestTask {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[3]")
      .appName("BotStreaming")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    val df = spark.read.json("/Users/ovoievodin/dev/prj/bot-stream/test.json")
    df.show()
  }
}
