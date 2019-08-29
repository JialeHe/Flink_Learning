package com.learn

import com.utils.CommonUtils
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.BroadcastStream

/**
  *
  * @author jiale.he
  * @date 2019/08/28
  * @email jiale.he@mail.hypers.com
  */
object MyBroadcastStateDemo {
  def main(args: Array[String]): Unit = {

    // 获取环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置checkpoint存储路径, 默认为内存存储
    env.setStateBackend(new FsStateBackend("..."))
    env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE)
    // 设置flink的重启策略, 重试4次, 一次1秒
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 1000))
    // 任务退出后保存checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置并行度
    env.setParallelism(1)

    // 从kafka读取数据,得到的DataStream是主流
    val dataStream: DataStream[String] = CommonUtils.getDataStream(env, "broadcast")
    // 初始化广播的流
    val hejialeBroadcastState: MapStateDescriptor[Long, Long] = new MapStateDescriptor[Long, Long]("hejiale", Types.of[Long], Types.of[Long])

    // 广播一个socket流
    val queryStream: BroadcastStream[String] = env.socketTextStream("mini04", 9999).broadcast(hejialeBroadcastState)

    val stream: KeyedStream[(String, Long), String] = dataStream.map(x => {
      val line: Array[String] = x.split(" ")
      (line(0), line(1).toLong)
    }).keyBy(_._1)

    stream.connect(queryStream)
      .process(new QueryFunction)
      .print()

    env.execute("Broadcast Demo")
  }
}

class QueryFunction extends KeyedBroadcastProcessFunction[String, (String, Long), String, Long] {
  override def processElement(in1: (String, Long), readOnlyContext: KeyedBroadcastProcessFunction[String, (String, Long), String, Long]#ReadOnlyContext, collector: Collector[Long]): Unit = {
    println("处理非广播的数据")
  }

  override def processBroadcastElement(in2: String, context: KeyedBroadcastProcessFunction[String, (String, Long), String, Long]#Context, collector: Collector[Long]): Unit = {
    println("处理广播的数据")
  }
}