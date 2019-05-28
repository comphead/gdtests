package com.av

import java.util
import java.util.Collections

import com.av.StreamManager.safeTo
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka010._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.slf4j.LoggerFactory

import scala.util.Try
import collection.JavaConverters._

object App {
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


trait StreamReader {
  def readStream(spark: SparkSession, kafkaParams: Map[String, Object])
}

trait DStreamReader extends StreamReader {
  override def readStream(spark: SparkSession, kafkaParams: Map[String, Object]) = {
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val checkpointDir = "/tmp/checkpoint"
    ssc.checkpoint(checkpointDir)

    val topics = List("bot.source")
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
      .print()
    //.filter(_.problem.nonEmpty)
    //.saveToCassandra("exam", "activity1", SomeColumns("ip", "problem", "ts", "id"))
    //      .flatMap {
    //        case r: Right[_, _] => r.right.toOption
    //        case l: Left[_, _] =>
    //          val left = l.left.get
    //          logger.error(s"Error processing [${left.originalMessage}] -> [${left.exception}]")
    //          None
    //        case _ => None
    //      }
    //      .saveToCassandra("exam", "activity", SomeColumns("type_", "ip", "unix_time", "category_id", "id"))


    //val stream2 = kafkaStream
    //.map(keyVal => safeTo[Data](keyVal.value()))
    //.flatMap(_.right.toOption)


    ssc.start
    ssc.awaitTermination
  }
}

trait StructuredStreamReader extends StreamReader {
  override def readStream(spark: SparkSession, kafkaParams: Map[String, Object]) = ???
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
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "bot.stream.group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    new DStreamReader {}.readStream(spark, kafkaParams)

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