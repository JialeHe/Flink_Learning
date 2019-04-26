package com.hjl

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  *
  * @author jiale.he
  * @date 2019/03/25
  * @email jiale.he@mail.hypers.com
  */
object WordCountBatchScala {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从字符串加载数据
    val text: DataSet[String] = env.fromElements("Who's there?","I think I hear them. Stand, ho! Who's there?")

    val count: AggregateDataSet[(String, Int)] = text.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    count.print()
  }
}
