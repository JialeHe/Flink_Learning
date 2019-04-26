package com.hjl

import java.util.Properties

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  *
  * @author jiale.he
  * @date 2019/03/26
  * @email jiale.he@mail.hypers.com
  */
object KafkaTest {
  private val zk = "master:2181"
  private val broker = "mini04:9092"
  private val group_id = "flink1"
  private val topic = "flink"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val backend: FsStateBackend = new FsStateBackend("hdfs://mini04:9000/flinkCheckpoint/",true)
    env.setStateBackend(backend)

    val pro = new Properties()
    pro.setProperty("zookeeper.connect",zk)
    pro.setProperty("bootstrap.servers",broker)
    pro.setProperty("group.id",group_id)

    val msg: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](topic,new SimpleStringSchema(),pro))
    println("获取kafka数据成功")
    msg.print()
    val res = msg.flatMap(_.split(" ")).map((_,1)).keyBy(0).timeWindow(Time.seconds(5)).sum(1)
    res.print()
    println("aaaaaaaaaaa")
    env.execute("Kafka Test")
  }
}
