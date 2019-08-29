package com.learn

import com.utils.CommonUtils
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.util.Collector

/**
  *
  * @author jiale.he
  * @date 2019/08/29
  * @email jiale.he@mail.hypers.com
  */
object FlinkWindowJoin {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 对象重用
    env.getConfig.enableObjectReuse()
    env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE)
    // 设置checkpoint Time Notion 默认为ProcessTime
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // kafka流
    val kafka: DataStream[String] = CommonUtils.getDataStream(env, "join_1")
    //    val stream1: DataStream[String] = CommonUtils.getDataStream(env, "join_2")

    // socket流
    val text: DataStream[String] = env.socketTextStream("mini04", 9999)

    val text_stream = text.map(_.split(" "))
      .map(x => (x(0), x(1)))
      .keyBy(_._1)

    val kafka_stream = kafka.map(_.split(" "))
      .map(x => (x(0), x(1)))

    // JOIN
    kafka_stream
      .join(text_stream)
      .where(_._1).equalTo(_._1)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
      .trigger(CountTrigger.of(1))
      .apply((t1, t2, out: Collector[String]) => {
        out.collect(t1._2 + " <=> " + t2._2)
      })

    // CoGroup
    kafka_stream
      .coGroup(text_stream)
      .where(_._1).equalTo(_._1)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
      .trigger(CountTrigger.of(1))
      .apply((t1, t2, out: Collector[String]) => {
        // t1和t2是迭代器, out输出
        //t1,t2是迭代器,out是输出
        val stringBuilder = new StringBuilder("Data in stream1: \n")
        for (i1 <- t1) {
          stringBuilder.append(i1._1 + "<=>" + i1._2 + "\n")
        }
        stringBuilder.append("Data in stream2: \n")
        for (i2 <- t2) {
          stringBuilder.append(i2._1 + "<=>" + i2._2 + "\n")
        }
        out.collect(stringBuilder.toString)
      })

    //FlatMap
    kafka_stream.connect(text_stream).flatMap(new CoFlatMapFunction[(String, String), (String, String), String] {
      // 处理kafka数据流
      override def flatMap1(in1: (String, String), out: Collector[String]): Unit = {
        out.collect(in1._1)
        println("kafka_stream: " + in1)
      }

      override def flatMap2(in2: (String, String), out: Collector[String]): Unit = {
        println("text_stream: " + in2)
      }
    }).print()
    env.execute("Join Demo")

  }
}
