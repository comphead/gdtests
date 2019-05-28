package com.av

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object KafkaProducer1 {
  def main(args: Array[String]): Unit = {
    val client = new KafkaProd with BotActivityGenerator
    val dispatcher = new MessageProducer(client)

    dispatcher.dispatch()
    client.close()

  }
}


trait Generator[F[_], T] {
  def generate(): F[T]
}

trait BotActivityGenerator extends Generator[Seq, String]{
  override def generate(): Seq[String] = {
    val ts = System.currentTimeMillis()
    Seq(
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.1", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.2", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.3", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.4", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.5", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.6", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.7", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.8", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.9", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.10", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.11", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1002, "ip": "172.10.0.11", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1003, "ip": "172.10.0.11", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1004, "ip": "172.10.0.11", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1005, "ip": "172.10.0.11", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1006, "ip": "172.10.0.11", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1007, "ip": "172.10.0.11", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.12", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.12", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.12", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.12", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.12", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.12", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.12", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.12", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.12", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.13", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.14", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.15", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.16", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.17", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.18", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.19", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.21", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.22", "type": "click"}""",
      s"""{"unix_time": $ts, "category_id": 1001, "ip": "172.10.0.23", "type": "click"}"""

    )
  }
}


trait Producer[V] extends Logger {
  def send(msgs: Seq[V]): Unit
  def close()
}

class MessageProducer(client: Producer[String] with Generator[Seq, String]) {
  def dispatch(): Unit = client.send(client.generate())
}

trait KafkaProd extends Producer[String] {

  import collection.JavaConverters._

  private val kafkaParams = Map[String, Object](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> "true",
    ProducerConfig.ACKS_CONFIG -> "all"
  )
  private val topic = "bot.source"
  lazy val client = new KafkaProducer[String, String](kafkaParams.asJava)

  override def send(msgs: Seq[String]): Unit = msgs.foreach(m => client.send(
    new ProducerRecord[String, String](topic, null, m),
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
        Option(exception).fold()(e => logger.error("Error producing -> ", e))
    }))

  override def close(): Unit = {
    client.flush()
    client.close()
  }

}

