package com.hjl

import java.util.Properties

import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09

/**
  *
  * @author jiale.he
  * @date 2019/04/04
  * @email jiale.he@mail.hypers.com
  */
object KafkaProducer {
  private val zk = "mini04:2181"
  private val broker = "mini04:9092"
  private val group_id = "flink_group"
  private val topic = "flink"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val pro = new Properties()
    pro.setProperty("zookeeper.connect", zk)
    pro.setProperty("bootstrap.servers", broker)
    pro.setProperty("group.id", group_id)

    val producer = new FlinkKafkaProducer09[String](topic, new SimpleStringSchema,pro)

    val msg: List[String] = List(
      "hypers",
      "Delivery",
      "Is",
      "Good"
    )





  }
}
