package com.av

import com.av.ConfigReader.{safeGetConf => cnf}
import com.av.SourceBuilder._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SparkSession, _}

object AppleTestTask {
  def main(args: Array[String]): Unit = {
    AppleTest()
  }
}

trait ConnectionFactory[A] {
  def instance(): A
}

trait SourceBuilder[A] {
  protected val options = new scala.collection.mutable.HashMap[String, String]

  def config(key: String, value: String): SourceBuilder[A] = synchronized {
    options += key -> value
    this
  }

  def build(): A
}


object ConnectionFactory {
  implicit val sparkSessionBuilder: ConnectionFactory[SparkSession] = new ConnectionFactory[SparkSession] {
    override def instance(): SparkSession = SparkSession.builder
      .master(cnf[String]("app.spark.host").getOrElse("local[3]"))
      .appName("BotStreaming")
      .config("spark.driver.memory", "2g")
      .getOrCreate()
  }
}

object SourceBuilder {
  val PATH = "path"

  case class File(path: String)

  implicit val fileBasedSource: SourceBuilder[File] = new SourceBuilder[File] {
    override def build(): File = File(options.getOrElse(PATH, ""))
  }
}

object AppleTest {

  def apply(): Unit = {
    val spark = getConnection[SparkSession].instance()

    import spark.sqlContext.implicits._
    val source = getSource[File].config(PATH, cnf[String]("app.source.csv.path").getOrElse("")).build()

    val customSchema = StructType(Array(
      StructField("category", StringType, nullable = true),
      StructField("product", StringType, nullable = true),
      StructField("userId", StringType, nullable = true),
      StructField("eventTime", TimestampType, nullable = true),
      StructField("eventType", StringType, nullable = true)))

    val df = spark
      .read
      .option("header", "true")
      .schema(customSchema)
      .csv(source.path)


    /**
      * Task #1(window)
      */

    val window = Window.partitionBy($"userId", $"category").orderBy($"eventTime")
    val windowBySession = Window.partitionBy($"userId", $"category", $"sessionId")

    val task1a =
      df
        .withColumn("prevEventTime",
          lag($"eventTime", 1).over(window))
        .withColumn("diffSeconds", unix_timestamp($"eventTime") - unix_timestamp($"prevEventTime"))
        .withColumn("initialSessionId", when($"diffSeconds".isNull || $"diffSeconds".gt(300), monotonically_increasing_id()))
        .withColumn("sessionId", last($"initialSessionId", ignoreNulls = true).over(window))
        .withColumn("nextSession", lead($"sessionId", 1).over(window))
        .withColumn("sessionEndTime_", when($"nextSession" =!= $"sessionId" || $"nextSession".isNull, lead($"eventTime", 1).over(window)))
        .withColumn("sessionEndTime__", max($"sessionEndTime_").over(windowBySession))
        .withColumn("sessionEndTime", when($"sessionEndTime__".isNull, max($"eventTime").over(windowBySession)).otherwise($"sessionEndTime__"))
        .withColumn("sessionStartTime", min($"eventTime").over(windowBySession))
        .select($"userId", $"category", $"product", $"eventTime", $"eventType", $"sessionId", $"sessionStartTime", $"sessionEndTime")


    /**
      * Task #1(spark aggr)
      */

    type T3 = (Int, Long, Long, String)

    class Aggr extends Aggregator[Row, (String, T3), String] with Serializable {

      private lazy val buffer = new scala.collection.mutable.HashMap[String, T3]()

      override def zero: (String, T3) = ("", (-1, -1L, -1L, ""))

      override def reduce(b: (String, T3), a: Row): (String, T3) = {
        val key = a.getString(a.fieldIndex("category")) + "#" + a.getString(a.fieldIndex("userId"))
        val ts = a.getTimestamp(a.fieldIndex("eventTime")).getTime

        val t = buffer.getOrElseUpdate(key, (key.hashCode, ts, ts, ""))
        val updated = t match {
          case (_, tts, _, _) if ts - tts >= 300 * 1000 =>
            t.copy(_1 = t._1 + 1, _2 = ts)
          case _ => t
        }

        buffer.put(key, updated.copy(_4 = updated._4 + "," + updated._1 + "#" + ts.toString))

        (key, buffer(key))
      }


      override def outputEncoder: Encoder[String] = Encoders.STRING

      override def merge(b1: (String, T3), b2: (String, T3)): (String, T3) = {
        val latest = b2._2._4
          .split(",").filter(_.nonEmpty).map(x => x.split("#") match {
          case Array(s1, s2) => (s1.toInt, s2.toLong)
        }).filter(_._1 == b2._2._1).maxBy(_._2)._2

        b2.copy(_2 = b2._2.copy(_3 = latest))
      }

      override def finish(reduction: (String, T3)): String = {
        val red = reduction._2
        s"""
           |{"sid": ${red._1}, "ss": ${red._2}, "se": ${red._3}}
         """.stripMargin
      }

      override def bufferEncoder: Encoder[(String, T3)] = Encoders.tuple(Encoders.STRING, Encoders.tuple(Encoders.scalaInt, Encoders.scalaLong, Encoders.scalaLong, Encoders.STRING))
    }

    df
      .filter($"userId" === "user 300" && $"category" === "notebooks")
      .groupBy($"userId", $"category", $"eventTime").agg(new Aggr().toColumn.as("aggr"))
      .orderBy($"eventTime")
      .select(
        $"userId", $"category", $"eventTime",
        get_json_object($"aggr", "$.sid").as("sessionId"),
        from_unixtime(get_json_object($"aggr", "$.ss") / 1000).as("sessionStart"),
        from_unixtime(get_json_object($"aggr", "$.se") / 1000).as("sessionEnd"))
      .show(false)

    val task2a = task1a
      .withColumn("sessionDuration", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))

    /**
      * Task #2
      * average session duration
      */


    task2a
      .select($"category", $"userId", $"sessionDuration")
      .distinct()
      .groupBy($"category").agg(avg($"sessionDuration"))


    /**
      * Task #2
      * users
      */

    task2a
      .select($"category", $"userId", $"sessionDuration")
      .distinct()
      .groupBy($"category", $"userId").agg(sum($"sessionDuration").as("sessionDuration"))
      .withColumn("durationCategory", when($"sessionDuration" < 60, "<1").when($"sessionDuration".between(60, 300), "1 to 5").otherwise(">5"))
      .groupBy($"category").pivot($"durationCategory").agg(count($"userId"))

    /**
      * Task #2
      * top10
      */

    df
      .withColumn("prevProduct", lag($"product", 1).over(window))
      .withColumn("initialSessionId", when($"prevProduct".isNull || $"prevProduct" =!= $"product", monotonically_increasing_id()))
      .withColumn("sessionId", last($"initialSessionId", ignoreNulls = true).over(window))
      .withColumn("nextEventTime", lead($"eventTime", 1).over(window))
      .withColumn("sessionEndTime", max($"nextEventTime").over(windowBySession))
      .withColumn("sessionStartTime", min($"eventTime").over(windowBySession))
      .select($"category", $"product", $"userId", $"sessionId", (unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime")).as("sessionDuration"))
      .distinct()
      .groupBy($"category", $"product").agg(sum($"sessionDuration").as("sessionDuration"))
      .withColumn("rank", rank().over(Window.partitionBy($"category").orderBy($"sessionDuration")))
      .filter($"rank".lt(10))

  }

  def getConnection[A](implicit conn: ConnectionFactory[A]): ConnectionFactory[A] = implicitly(conn)

  def getSource[A](implicit src: SourceBuilder[A]): SourceBuilder[A] = implicitly(src)
}

object ConfigReader {
  private lazy val conf = ConfigFactory.load("reference.conf")

  def safeGetConf[T](path: String): Option[T] = conf.getObjectOptional(path)

  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getObjectOptional[T](path: String): Option[T] =
      if (underlying.hasPath(path)) Some(underlying.getAnyRef(path).asInstanceOf[T]) else None
  }

}

