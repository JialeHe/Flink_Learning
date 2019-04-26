package com.hjl

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * @author jiale.he
  * @date 2019/03/25
  * @email jiale.he@mail.hypers.com
  */
object SocketWindowWordCountScala {
  def main(args: Array[String]): Unit = {

    // 定一一个数据类型保存单词出现的次数
    case class WordWithCount(word: String, count: Long)
    // port 连接的端口
    val port: Int = ParameterTool.fromArgs(args).getInt("port")

    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 连接port获取输入数据
    val text: DataStream[String] = env.socketTextStream("mini04",port,'\n')

    import org.apache.flink.api.scala._
    val windowCount: DataStream[WordWithCount] = text.flatMap(word => word.split(" "))
      .map(word => WordWithCount(word, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    windowCount.print().setParallelism(1)
    env.execute("Socket Window WordCount")

  }
}
