package com.learn

import java.text.SimpleDateFormat

import com.utils.CommonUtils
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  *
  * @author jiale.he
  * @date 2019/08/24
  * @email jiale.he@mail.hypers.com
  */
object MyWatermarkDamo {

  def main(args: Array[String]): Unit = {

    val log = LoggerFactory.getLogger("MyWatermarkDamo")
    // 获取Flink streaming运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取checkpoint
    val checkpoint = env.getCheckpointConfig
    // 设置任务取消或失败的时候,不会自动清除checkpoint
    checkpoint.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /**
      * 规定两次checkpoint之间的最小时间为了应用可以再此期间可以有明显的处理进度
      * 假设这个值为5秒
      * 说明在一个checkpoint结束之后的5秒内不会有新的checkpoint被触发
      * 这就意味着checkpoint interval(checkpoint的时间间隔)的值要比这个值要大
      * 为什么设置这个值
      * 因为checkpoint interval有时候会不可靠,比如当文件系统比较缓慢的时候,checkpoint花费的时间会比较大
      * 这样子的的话,只有checkpoint interval的话就会重叠,
      * 设置minimum time between checkpoint也要求checkpoint的并发度为1
      */
    checkpoint.setMinPauseBetweenCheckpoints(5000L)
    // 设置在checkpoint失败的时候,任务会继续运行
    checkpoint.setFailOnCheckpointingErrors(false)
    // 设置在同一时间只运行一个checkpoint
    checkpoint.setMaxConcurrentCheckpoints(1)
    // 设置checkpoint必须要在20秒之内完成, 否则就丢弃
    checkpoint.setCheckpointTimeout(20000L)
    // 设置时间TIme Notion 为EventTime, 默认使用ProcessTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置checkpoint时间为10秒一次, 使用checkpoint语义为exactly-once
    env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE)
    // 设置Flink的重启策略, 重试4次, 每次间隔10秒
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000L))
    env.setParallelism(1)

    val dataStream: DataStream[String] = CommonUtils.getDataStream(env,"flink_test")

    dataStream
      .filter(x => (x.nonEmpty && x.contains(",")))
      .map(x => {
        val arr: Array[String] = x.split(",")
        val key: String = arr(0)
        log.info("Trace key: " + key)
        log.info("Trace timestamp: " + arr(1))
        val timestamp = CommonUtils.checkLong(arr(1))
        (key, timestamp)
      })
      // 获取数据中的时间戳, 生成Watermark
      .assignTimestampsAndWatermarks(new MyTimestampExtractor)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new MyWindowFunctionTest)
      .print()

    env.execute("WaterMark tesk")

  }
}

class MyTimestampExtractor extends AssignerWithPeriodicWatermarks[(String, Long)] with Serializable {

  var currentMaxTimeStamp = 0L
  // 允许最大时间乱序为10秒
  val maxOutOfOrderness = 10000L

  var wm: Watermark = null
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")

  override def getCurrentWatermark: Watermark = {
    wm = new Watermark(currentMaxTimeStamp - maxOutOfOrderness)
    wm
  }

  override def extractTimestamp(t: (String, Long), l: Long): Long = {
    val key: String = t._1
    val timestamp: Long = t._2
    currentMaxTimeStamp = Math.max(timestamp, currentMaxTimeStamp)
    println("key:" + t._1)
    println("timestamp: " + timestamp + "\tformat: " + sdf.format(timestamp))
    println("currentMaxTimestamp: " + currentMaxTimeStamp + "\tformat: " + sdf.format(currentMaxTimeStamp))
    println("watermark: " + currentMaxTimeStamp + "\tformat: " + sdf.format(wm.getTimestamp))
    timestamp
  }
}

/**
  * IN The type of the input value.
  * OUT The type of the output value.
  * KEY The type of the key.
  */
class MyWindowFunctionTest extends WindowFunction[(String, Long), (String, Int, String, String, String, String), String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int, String, String, String, String)]): Unit = {
    //输入参数是一个迭代器,根据时间戳排序
    val list = input.toList.sortBy(_._2)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    //输出
    out.collect(key, input.size, format.format(list.head._2), format.format(list.last._2), format.format(window.getStart), format.format(window.getEnd))
  }
}