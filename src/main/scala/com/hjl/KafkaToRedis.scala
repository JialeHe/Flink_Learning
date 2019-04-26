package com.hjl


import java.util.Properties

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.log4j.Logger

/**
  *
  * @author jiale.he
  * @date 2019/03/25
  * @email jiale.he@mail.hypers.com
  */
object KafkaToRedis {
  private val zk = "mini04:2181"
  private val broker = "mini04:9092"
  private val group_id = "flink_group"
  private val topic = "flink"

  def main(args: Array[String]): Unit = {

    val log: Logger = Logger.getLogger(KafkaToRedis.getClass)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    // Exactly-Once
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val backend: FsStateBackend = new FsStateBackend("hdfs://mini04:9000/flinkCheckpoint/",true)
    env.setStateBackend(backend)
    val pro = new Properties()
    pro.setProperty("zookeeper.connect", zk)
    pro.setProperty("bootstrap.servers", broker)
    pro.setProperty("group.id", group_id)
    // 获得消费者对象(数据源)
    println("===============》 Starting  ==============》")
    val consumer = new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema, pro)

    val stream: DataStream[String] = env.addSource(consumer)
    val wordCount: DataStream[(String, String)] = stream
      .flatMap(_.split(" "))
      .filter(x => x != null)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .setParallelism(2)
      .map(x => (x._1, x._2.toString))



    val conf = new FlinkJedisPoolConfig.Builder().setHost("mini04").setPort(6379).build()
    val sink = new RedisSink[(String, String)](conf, new RedisExampleMapper)

    wordCount.addSink(sink)

    env.execute("Flink Streaming To Redis")
  }

}

class RedisExampleMapper extends RedisMapper[(String, String)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "flinkTest")
  }

  override def getKeyFromData(t: (String, String)): String = t._1

  override def getValueFromData(t: (String, String)): String = t._2
}
