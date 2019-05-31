package com.av

import com.av.StreamManager.safeTo
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory

object App {
  org.apache.log4j.BasicConfigurator.configure()
  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
  val topic = "bot.source"

  def main(args: Array[String]): Unit = {


    StreamManager.start()
  }
}

case class Data(`type`: String, ip: String, unix_time: Long, category_id: Long, id: Long) extends Serializable

case class RawData(`type`: String, ip: String, unix_time: Long, category_id: Long) extends Serializable

case class UnparsableData(id: Option[Long], originalMessage: String, exception: Throwable)

case class AggregateData(totalViews: Int, totalClicks: Int, totalCategories: Int, numCalls: Int) {
  def activityRatio = totalClicks / Math.max(1, totalViews)
}

case class EvaluatedActivity(ip: String, problem: String, ts: Long, id: Long)


trait StreamReader extends Logger {
  def readStream(spark: SparkSession, kafkaParams: Map[String, Object])
}

trait DStreamReader extends StreamReader {
  override def readStream(spark: SparkSession, kafkaParams: Map[String, Object]) = {
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val checkpointDir = "/tmp/checkpoint"
    ssc.checkpoint(checkpointDir)

    val topics = List(App.topic)
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    kafkaStream
      .map(record => safeTo[RawData](record.value()))
      .flatMap(_.right.toOption)
      .map(a => {
        (a.ip, a)
      })
      .mapValues(act => List(act))
      .reduceByKeyAndWindow((a, b) => a ++ b, (a, b) => a diff b, Minutes(1), Minutes(1))
      .mapWithState(
        StateSpec.function((ip: String, newAct: Option[List[RawData]],
                            aggData: State[AggregateData]) => {
          val newTxs = newAct.getOrElse(List.empty[RawData])
          val newAgg = AggregateData(
            totalViews = newTxs.count(_.`type` == "view"),
            totalClicks = newTxs.count(_.`type` == "click"),
            totalCategories = newTxs.map(_.category_id).distinct.size,
            numCalls = newTxs.size
          )

          //          aggData.update(aggData.getOption().fold(newAgg)(a =>
          //            AggregateData(
          //              totalViews = a.totalViews + newAgg.totalViews,
          //              totalClicks = a.totalClicks + newAgg.totalClicks,
          //              totalCategories = a.totalCategories + newAgg.totalCategories,
          //              numCalls = a.numCalls + newAgg.numCalls
          //            )))

          val ts = System.currentTimeMillis()

          EvaluatedActivity(
            ip = ip,
            problem = newAgg match {
              case n if n.totalCategories > 5 => "Frequent category switch"
              case n if n.numCalls > 1000 => "Enormous event rate"
              case n if n.activityRatio > 3 => "Suspiciuos view/clicks ratio"
              case _ => ""
            },
            ts = ts,
            id = System.nanoTime())
        })
      )
      .filter(_.problem.nonEmpty)
      .saveToCassandra("exam", "activity1", SomeColumns("ip", "problem", "ts", "id"))


    ssc.start
    ssc.awaitTermination
  }
}

import org.apache.spark.sql.functions._

trait StructuredStreamReader extends StreamReader {
  override def readStream(spark: SparkSession, kafkaParams: Map[String, Object]) = {
    import spark.sqlContext.implicits._

    val struct = new StructType()
      .add("type", DataTypes.StringType)
      .add("ip", DataTypes.StringType)
      .add("unix_time", DataTypes.LongType)
      .add("category_id", DataTypes.LongType)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaParams(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).toString)
      .option("subscribe", App.topic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", struct).as("activity"))
      .selectExpr("activity.type", "activity.ip", "activity.unix_time", "activity.category_id", "CAST(activity.unix_time as timestamp) timestamp")
      .withWatermark("timestamp", "1 minutes")
      .groupBy(
        window($"timestamp", "1 minutes", "1 minutes"),
        $"ip")
      .agg(
        count($"ip").as("numCalls"),
        approx_count_distinct($"category_id").as("totalCategories"),
        sum(when($"type" === "click", 1).otherwise(0)).as("totalClicks"),
        sum(when($"type" === "view", 1).otherwise(0)).as("totalViews")
      )
      .withColumn("problem",
        when($"totalCategories".gt(5), "Frequent category switch")
          .when($"numCalls".gt(1000), "Enormous event rate")
          .when(($"totalClicks" / greatest($"totalViews", lit(1))).gt(3), "Suspiciuos view/clicks ratio")
      )
      .select($"ip", $"problem", current_timestamp().cast(DataTypes.LongType).as("ts"), (regexp_replace($"ip", "\\.", "").cast(DataTypes.LongType) + current_timestamp().cast(DataTypes.LongType)).as("id"))
      .filter($"problem".isNotNull)


    //    val consoleOutput = df.writeStream
    //      .outputMode("append")
    //      .format("console")
    //      .start()

    import org.apache.spark.sql.cassandra._

    df.writeStream
      .foreachBatch { (batchDF, _) =>
        batchDF
          .write
          .cassandraFormat("activity1", "exam")
          .mode("append")
          .save
      }.start

    spark.streams.awaitAnyTermination()
  }

  class CassandraSink(sparkConf: SparkConf) extends ForeachWriter[Row] {
    def open(partitionId: Long, version: Long): Boolean = true

    def process(row: Row) = {
      def buildStatement: Statement =
        QueryBuilder.insertInto("exam", "activity1")
          .value("ip", row.getAs[String]("ip"))
          .value("problem", row.getAs[String]("problem"))
          .value("ts", System.currentTimeMillis())
          .value("id", System.nanoTime())

      CassandraConnector(sparkConf).withSessionDo { session =>
        session.execute(buildStatement)
      }
    }

    def close(errorOrNull: Throwable) = logger.error("Cassandra error", errorOrNull)
  }

}

trait StreamWriter[A] {
  def writeStream(stream: A): A
}

trait CassandraStreamWriter[A] extends StreamWriter[A] {
  override def writeStream(stream: A): A = ???
}


trait Logger {
  val logger = LoggerFactory.getLogger(getClass.getName)
}

object StreamManager extends Logger {
  def start(): Unit = {
    val spark = SparkSession.builder
      .master("local[3]")
      .appName("BotStreaming")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "bot.stream.group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    new StructuredStreamReader {}.readStream(spark, kafkaParams)
    //new DStreamReader {}.readStream(spark, kafkaParams)

  }

  import scala.util.{Either, Left, Right}

  implicit val formats = DefaultFormats

  def safeTo[T](js: String)(implicit manifest: Manifest[T]): Either[UnparsableData, T] = Try(js) {
    parse(js).extract[T]
  }

  def Try[A](js: String)(a: => A): Either[UnparsableData, A] = try Right(a)
  catch {
    case e: Exception =>
      logger.error(s"Error processing [$js] -> [${e.getMessage}]")
      Left(UnparsableData(None, js, e))
  }
}