package com.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010


/**
  *
  * @author jiale.he
  * @date 2019/08/24
  * @email jiale.he@mail.hypers.com
  */
object CommonUtils {

  private final val zk = "mini04:2181"
  private final val broker = "mini04:9092"
  private final val group_id = "hjl_lean"


  def getDataStream(env: StreamExecutionEnvironment, topic: String): DataStream[String] = {
    val pro = new Properties()
    pro.setProperty("zookeeper.connect", zk)
    pro.setProperty("bootstrap.servers", broker)
    pro.setProperty("group.id", group_id)
    val consumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), pro)
    env.addSource(consumer)
  }

  def checkLong(str: String): Long = {
    try {
      str.toLong
    } catch {
      case _: Exception => 1566644148900L
    }
  }

}
